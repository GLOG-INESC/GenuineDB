#!/usr/bin/env bash
set -euo pipefail


#############################################
# DRY RUN HANDLING
#############################################

DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  echo "**** DRY-RUN MODE ENABLED ****"
  echo "No AWS commands will be executed."
  echo
fi

# -------------------------
# Configuration - EDIT THIS
# -------------------------
# List of regions (order matters for CIDR calculation)
REGIONS=("us-east-1" "us-east-2" "eu-west-1" "eu-west-2" "ap-northeast-1" "ap-northeast-2" "ap-southeast-1" "ap-southeast-2")  # <-- replace with your regions

# Optional tag values
TAG_OWNER="vpc-mesh-script"
TAG_PROJECT="vpc-mesh"

# AWS CLI profile (uncomment and set if you want to use a specific profile)
# AWS_PROFILE="myprofile"
AWS_PROFILE_ARG="" # e.g. "--profile ${AWS_PROFILE}"

# -------------------------
# End configuration
# -------------------------

# Utility: run aws with region and optional profile
aws_cmd() {
  # args: region, ...aws args...
  local region="$1"; shift
  if $DRY_RUN; then
    echo "[DRY-RUN] aws --region ${region} $*"
  else
    if [[ -n "${AWS_PROFILE_ARG}" ]]; then
      aws ${AWS_PROFILE_ARG} --region "${region}" "$@"
    else
      aws --region "${region}" "$@"
    fi
  fi
}

# Indexed arrays to store resource ids by index
declare -A VPC_ID_BY_REGION
declare -A VPC_CIDR_BY_REGION
declare -A SUBNET_ID_BY_REGION
declare -A IGW_ID_BY_REGION
declare -A RTB_ID_BY_REGION

echo "Starting VPC/subnet/igw creation in ${#REGIONS[@]} regions..."

index=1
for region in "${REGIONS[@]}"; do
  echo "==> Processing region [$index] ${region}"

  # Compute XX = index*10 (so index=1 -> 10)
  XX=$(((index+3) * 10))
  CIDR="10.${XX}.0.0/16"
  VPC_CIDR_BY_REGION["$region"]="${CIDR}"
  echo "    CIDR for $region -> ${CIDR}"

  # 1) Create VPC
  vpc_json=$(aws_cmd "${region}" ec2 create-vpc --cidr-block "${CIDR}" --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${TAG_PROJECT}-${region}},{Key=Owner,Value=${TAG_OWNER}}]" --output json)
  vpc_id=$(echo "${vpc_json}" | jq -r '.Vpc.VpcId')
  echo "    Created VPC: ${vpc_id}"
  VPC_ID_BY_REGION["$region"]="${vpc_id}"

  # Enable DNS hostnames (useful)
  aws_cmd "${region}" ec2 modify-vpc-attribute --vpc-id "${vpc_id}" --enable-dns-hostnames "{\"Value\":true}" >/dev/null

  # 2) Create subnet in AZ 'a' (availability zone suffix 'a')
  # We need a full AZ name; get first AZ that ends with 'a' in the region
  az_list_json=$(aws_cmd "${region}" ec2 describe-availability-zones --filters Name=region-name,Values="${region}" --output json)
  az_a=$(echo "${az_list_json}" | jq -r '.AvailabilityZones[] | select(.ZoneName|endswith("a")) | .ZoneName' | head -n1)
  if [[ -z "${az_a}" ]]; then
    # fallback: take first AZ
    az_a=$(echo "${az_list_json}" | jq -r '.AvailabilityZones[0].ZoneName')
    echo "    Warning: couldn't find AZ ending with 'a' exactly; using ${az_a}"
  else
    echo "    Using AZ ${az_a} for subnet"
  fi

  # choose a /24 from the VPC space for the subnet, e.g. 10.XX.0.0/24
  SUBNET_CIDR="10.${XX}.0.0/24"
  subnet_json=$(aws_cmd "${region}" ec2 create-subnet --vpc-id "${vpc_id}" --cidr-block "${SUBNET_CIDR}" --availability-zone "${az_a}" --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${TAG_PROJECT}-${region}-subnet},{Key=Owner,Value=${TAG_OWNER}}]" --output json)
  subnet_id=$(echo "${subnet_json}" | jq -r '.Subnet.SubnetId')
  SUBNET_ID_BY_REGION["$region"]="${subnet_id}"
  echo "    Created Subnet ${subnet_id} (${SUBNET_CIDR}) in ${az_a}"

  # 3) Create Internet Gateway and attach
  igw_json=$(aws_cmd "${region}" ec2 create-internet-gateway --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${TAG_PROJECT}-${region}-igw},{Key=Owner,Value=${TAG_OWNER}}]" --output json)
  igw_id=$(echo "${igw_json}" | jq -r '.InternetGateway.InternetGatewayId')
  IGW_ID_BY_REGION["$region"]="${igw_id}"
  echo "    Created IGW ${igw_id}"
  aws_cmd "${region}" ec2 attach-internet-gateway --internet-gateway-id "${igw_id}" --vpc-id "${vpc_id}"
  echo "    Attached IGW to VPC"

  # 4) Create a route table and add a route for internet; associate with subnet
  rtb_json=$(aws_cmd "${region}" ec2 create-route-table --vpc-id "${vpc_id}" --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=${TAG_PROJECT}-${region}-rtb},{Key=Owner,Value=${TAG_OWNER}}]" --output json)
  rtb_id=$(echo "${rtb_json}" | jq -r '.RouteTable.RouteTableId')
  RTB_ID_BY_REGION["$region"]="${rtb_id}"
  echo "    Created Route Table ${rtb_id}"

  # create route for 0.0.0.0/0 -> IGW
  aws_cmd "${region}" ec2 create-route --route-table-id "${rtb_id}" --destination-cidr-block "0.0.0.0/0" --gateway-id "${igw_id}" >/dev/null
  echo "    Added 0.0.0.0/0 -> ${igw_id}"

  # associate route table with subnet
  assoc_json=$(aws_cmd "${region}" ec2 associate-route-table --route-table-id "${rtb_id}" --subnet-id "${subnet_id}" --output json)
  assoc_id=$(echo "${assoc_json}" | jq -r '.AssociationId')
  echo "    Associated route table ${rtb_id} with subnet ${subnet_id} (assoc ${assoc_id})"

  index=$((index + 1))
done

echo
echo "All VPCs, subnets, IGWs and route tables created."
echo

# -------------------------
# Create VPC peering connections (full mesh)
# -------------------------
echo "Creating VPC peering connections (full mesh) between regions..."

# We'll create a peering from region_i -> region_j for i<j and then accept it in region_j.
# We'll track peering IDs by key "${from_region}__${to_region}"
declare -A PCX_ID_BY_PAIR

n=${#REGIONS[@]}
for ((i=0;i<n;i++)); do
  for ((j=i+1;j<n;j++)); do
    from_region=${REGIONS[$i]}
    to_region=${REGIONS[$j]}
    from_vpc=${VPC_ID_BY_REGION["$from_region"]}
    to_vpc=${VPC_ID_BY_REGION["$to_region"]}
    echo "-> Creating peering: ${from_region}:${from_vpc}  <-->  ${to_region}:${to_vpc}"

    # create-vpc-peering-connection in from_region, peer-region = to_region
    pcx_json=$(aws_cmd "${from_region}" ec2 create-vpc-peering-connection \
      --vpc-id "${from_vpc}" \
      --peer-vpc-id "${to_vpc}" \
      --peer-region "${to_region}" \
      --tag-specifications "ResourceType=vpc-peering-connection,Tags=[{Key=Name,Value=${TAG_PROJECT}-pcx-${from_region}-to-${to_region}},{Key=Owner,Value=${TAG_OWNER}}]" \
      --output json)

    pcx_id=$(echo "${pcx_json}" | jq -r '.VpcPeeringConnection.VpcPeeringConnectionId')
    echo "    Created PCX ${pcx_id} (request exists in ${from_region})"
    PCX_ID_BY_PAIR["${from_region}__${to_region}"]="${pcx_id}"

    # Accept the peering in the accepter region (to_region)
    echo "    Accepting PCX ${pcx_id} in region ${to_region}..."
    aws_cmd "${to_region}" ec2 accept-vpc-peering-connection --vpc-peering-connection-id "${pcx_id}" >/dev/null

    # Wait until status is active
    echo "    Waiting for status to become 'active'..."
    aws_cmd "${from_region}" ec2 wait vpc-peering-connection-available --vpc-peering-connection-ids "${pcx_id}"
    # double-check status
    status=$(aws_cmd "${from_region}" ec2 describe-vpc-peering-connections --vpc-peering-connection-ids "${pcx_id}" --output json | jq -r '.VpcPeeringConnections[0].Status.Code')
    echo "    Peering ${pcx_id} status: ${status}"

    # store reverse mapping too for easy lookup later
    PCX_ID_BY_PAIR["${to_region}__${from_region}"]="${pcx_id}"
  done
done

echo
echo "All peering connections created and accepted."

# -------------------------
# Add routes for peer networks in each route table
# -------------------------
echo "Adding routes in each route table for all remote VPC CIDRs via peering connections..."

for region in "${REGIONS[@]}"; do
  rtb_id=${RTB_ID_BY_REGION["$region"]}
  this_vpc_cidr=${VPC_CIDR_BY_REGION["$region"]}
  echo "Region ${region}: route table ${rtb_id} (local CIDR ${this_vpc_cidr})"

  for other_region in "${REGIONS[@]}"; do
    if [[ "${other_region}" == "${region}" ]]; then
      continue
    fi
    other_cidr=${VPC_CIDR_BY_REGION["$other_region"]}
    pcx_key="${region}__${other_region}"
    pcx_id=${PCX_ID_BY_PAIR["$pcx_key"]:-}

    if [[ -z "${pcx_id}" ]]; then
      echo "    Warning: no PCX id found for ${pcx_key}, skipping route to ${other_cidr}"
      continue
    fi

    # create route: destination other_cidr -> pcx_id (in 'region')
    echo "    Creating route to ${other_cidr} via ${pcx_id} ..."
    # If a route already exists for the destination, replace it
    set +e
    aws_cmd "${region}" ec2 replace-route --route-table-id "${rtb_id}" --destination-cidr-block "${other_cidr}" --vpc-peering-connection-id "${pcx_id}" >/dev/null 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]; then
      # try create-route instead
      aws_cmd "${region}" ec2 create-route --route-table-id "${rtb_id}" --destination-cidr-block "${other_cidr}" --vpc-peering-connection-id "${pcx_id}" >/dev/null
    fi
    set -e
    echo "      -> route to ${other_cidr} added."
  done
done

echo
echo "All routes created. Summary:"
for region in "${REGIONS[@]}"; do
  echo "Region ${region}: VPC=${VPC_ID_BY_REGION[$region]}, CIDR=${VPC_CIDR_BY_REGION[$region]}, Subnet=${SUBNET_ID_BY_REGION[$region]}, IGW=${IGW_ID_BY_REGION[$region]}, RTB=${RTB_ID_BY_REGION[$region]}"
done

echo
echo "Script complete. You now have a full-mesh (point-to-point between every region) VPC peering setup and route tables configured."

# End of script
