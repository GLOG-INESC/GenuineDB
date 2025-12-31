#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CONFIGURATION
# ============================================================
# Define each region + its VPC metadata
# Format: REGION|CIDR|VPC_ID|ROUTE_TABLE_ID
VPCS=(
  "us-east-1|10.10.0.0/16|vpc-0f26f1db9ac377f20|rtb-0e34b56c7ab2b14a2"
  "us-east-2|10.30.0.0/16|vpc-0d82e1a3c85bddd02|rtb-06f266c247458abf9"
  "eu-west-1|10.40.0.0/16|vpc-04e44778c419ed9d7|rtb-075d9d56defbf76ca"
  "eu-west-2|10.50.0.0/16|vpc-0986326c44d30f0eb|rtb-03df2c64841c73cc7"
  "ap-northeast-1|10.60.0.0/16|vpc-0968616f68265770a|rtb-0983457c530fc569a"
  "ap-northeast-2|10.70.0.0/16|vpc-0bef4db2064d1a688|rtb-00d44cc6ce22f2ae5"
  "ap-southeast-1|10.80.0.0/16|vpc-012a2da08070fc33a|rtb-0127273d4bdea00d2"
  "ap-southeast-2|10.90.0.0/16|vpc-0798c4ee703720524|rtb-083af837cfeddcd90"
)

TAG_OWNER="vpc-mesh-script"
TAG_PROJECT="vpc-mesh"

# ============================================================
# INTERNAL ARRAYS
# ============================================================
declare -A REGION_CIDR
declare -A REGION_VPC
declare -A REGION_RTB
declare -A PCX_ID_BY_PAIR

# ============================================================
# PARSE THE CONFIG MAP
# ============================================================
echo "Loading VPC configuration..."
for entry in "${VPCS[@]}"; do
  region=$(echo "$entry" | cut -d "|" -f1)
  cidr=$(echo "$entry"   | cut -d "|" -f2)
  vpc=$(echo "$entry"    | cut -d "|" -f3)
  rtb=$(echo "$entry"    | cut -d "|" -f4)

  REGION_CIDR["$region"]="$cidr"
  REGION_VPC["$region"]="$vpc"
  REGION_RTB["$region"]="$rtb"

  echo " - $region: VPC=$vpc CIDR=$cidr RTB=$rtb"
done
echo

# ============================================================
# CREATE PEERING CONNECTIONS
# ============================================================
regions=("${!REGION_CIDR[@]}")
n=${#regions[@]}

echo "Creating VPC Peering Connections..."
echo

for ((i=0; i<n; i++)); do
  for ((j=i+1; j<n; j++)); do

    r1=${regions[$i]}
    r2=${regions[$j]}

    if { [[ "$r1" == "us-east-1" && "$r2" == "us-east-2" ]] ||
         [[ "$r1" == "us-east-2" && "$r2" == "us-east-1" ]]; }; then
        echo "Skipping peering and route creation between $r1 and $r2"
        continue
    fi

    vpc1=${REGION_VPC[$r1]}
    vpc2=${REGION_VPC[$r2]}

    echo "== Peering: $r1 ($vpc1) <--> $r2 ($vpc2)"

    existing_pcx=$(aws --region "$r1" ec2 describe-vpc-peering-connections \
      --filters \
        "Name=requester-vpc-info.vpc-id,Values=${vpc1}" \
        "Name=accepter-vpc-info.vpc-id,Values=${vpc2}" \
      --query "VpcPeeringConnections[?Status.Code!='deleted'].VpcPeeringConnectionId" \
      --output text 2>/dev/null)

    if [[ -z "$existing_pcx" ]]; then
        # Also check reverse direction (some people mix requester/acceptor order)
        existing_pcx=$(aws --region "$r1" ec2 describe-vpc-peering-connections \
          --filters \
            "Name=requester-vpc-info.vpc-id,Values=${vpc2}" \
            "Name=accepter-vpc-info.vpc-id,Values=${vpc1}" \
          --query "VpcPeeringConnections[?Status.Code!='deleted'].VpcPeeringConnectionId" \
          --output text 2>/dev/null)
    fi

    if [[ -n "$existing_pcx" ]]; then
        echo "Skipping: Existing peering connection detected between $vpc1 and $vpc2 → $existing_pcx"
        PCX_ID_BY_PAIR["$r1|$r2"]="$existing_pcx"
        PCX_ID_BY_PAIR["$r2|$r1"]="$existing_pcx"
        continue
    fi

    # Create PCX
    pcx_id=$(aws --region "$r1" ec2 create-vpc-peering-connection \
      --vpc-id "$vpc1" \
      --peer-vpc-id "$vpc2" \
      --peer-region "$r2" \
      --tag-specifications "ResourceType=vpc-peering-connection,Tags=[{Key=Name,Value=${TAG_PROJECT}-${r1}-to-${r2}},{Key=Owner,Value=${TAG_OWNER}}]" \
      --query "VpcPeeringConnection.VpcPeeringConnectionId" \
      --output text)

    echo "   PCX ID: $pcx_id"

    echo "   Waiting for PCX to propagate to region ${r2}..."

    max_attempts=10
    attempt=1
    accepted=false

    while [[ $attempt -le $max_attempts ]]; do
      echo "     Attempt $attempt to accept PCX in ${r2}..."

      if aws --region "$r2" ec2 accept-vpc-peering-connection \
          --vpc-peering-connection-id "$pcx_id" >/dev/null 2>&1; then
          accepted=true
          echo "   Peering accepted in ${r2}"
          break
      else
          echo "     PCX not found yet; waiting 5s..."
          sleep 5
      fi

      attempt=$((attempt+1))
    done

    if [[ "$accepted" == false ]]; then
      echo "ERROR: Failed to accept PCX ${pcx_id} in region ${r2} after ${max_attempts} attempts."
      exit 1
    fi

    PCX_ID_BY_PAIR["$r1|$r2"]="$pcx_id"
    PCX_ID_BY_PAIR["$r2|$r1"]="$pcx_id"

    echo "   Peering active."

  done
done

echo
echo "All peerings created and accepted."
echo

# ============================================================
# UPDATE ROUTE TABLES
# ============================================================
echo "Updating route tables..."

for r1 in "${regions[@]}"; do
  rtb=${REGION_RTB[$r1]}

  echo
  echo "Route Table for region: $r1 → $rtb"

  for r2 in "${regions[@]}"; do
    [[ "$r1" == "$r2" ]] && continue

    if { [[ "$r1" == "us-east-1" && "$r2" == "us-east-2" ]] ||
             [[ "$r1" == "us-east-2" && "$r2" == "us-east-1" ]]; }; then
            echo "Skipping peering and route creation between $r1 and $r2"
            continue
    fi
    cidr=${REGION_CIDR[$r2]}
    pcx=${PCX_ID_BY_PAIR["$r1|$r2"]}

    echo " - Adding route to $cidr via $pcx"

    aws --region "$r1" ec2 create-route \
      --route-table-id "$rtb" \
      --destination-cidr-block "$cidr" \
      --vpc-peering-connection-id "$pcx" >/dev/null 2>&1 || \
    aws --region "$r1" ec2 replace-route \
      --route-table-id "$rtb" \
      --destination-cidr-block "$cidr" \
      --vpc-peering-connection-id "$pcx"
  done
done

echo
echo "=========================================="
echo "    Full Peering Mesh Successfully Built"
echo "=========================================="
