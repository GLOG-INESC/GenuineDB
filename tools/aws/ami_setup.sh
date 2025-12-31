#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# CONFIGURATION
# -----------------------------
SOURCE_REGION="us-east-1"              # Region where the original AMI exists
SOURCE_AMI_ID="ami-0776e709531e999af"   # Original AMI ID

TARGET_REGIONS=("ap-northeast-2" "ap-southeast-1" "ap-southeast-2")  # <-- replace with your regions
KEY_PAIR_NAME="AWSGeoZip"             # Existing EC2 Key Pair
SECURITY_GROUP_NAME="all-traffic-sg"
LAUNCH_TEMPLATE_NAME_PREFIX="my-launch-template"
ROOT_VOLUME_SIZE=100                   # GB

TAG_KEY="CopiedFrom"
TAG_VALUE="${SOURCE_AMI_ID}"

SUBNETS=(
  "us-east-1|vpc-0f26f1db9ac377f20|subnet-03396ead40f96130c|us-east-1a"
  "us-east-2|vpc-0d82e1a3c85bddd02|subnet-0ca7babd8dc80e854|us-east-2a"
  "eu-west-1|vpc-04e44778c419ed9d7|subnet-06b74978ea20a2db3|eu-west-1a"
  "eu-west-2|vpc-0986326c44d30f0eb|subnet-070451818d51f9eb7|eu-west-2a"
  "ap-northeast-1|vpc-0968616f68265770a|subnet-09adf281af24b9341|ap-northeast-1a"
  "ap-northeast-2|vpc-0bef4db2064d1a688|subnet-0a707de254e5a7f32|ap-northeast-2a"
  "ap-southeast-1|vpc-012a2da08070fc33a|subnet-0a769ba2b1213db2d|ap-southeast-1a"
  "ap-southeast-2|vpc-0798c4ee703720524|subnet-0d95f2771c56c4cdd|ap-southeast-2a"
)

# -----------------------------
# PARSE VPC AND SUBNET INFO
# -----------------------------
declare -A REGION_VPC
declare -A REGION_SUBNET
declare -A REGION_AZ

for subnet in "${SUBNETS[@]}"; do
  region=$(echo "$subnet" | cut -d '|' -f1)
  REGION_VPC["$region"]=$(echo "$subnet" | cut -d '|' -f2)
  REGION_SUBNET["$region"]=$(echo "$subnet" | cut -d '|' -f3)
  REGION_AZ["$region"]=$(echo "$subnet" | cut -d '|' -f4)
done

# -----------------------------
# MAIN LOOP
# -----------------------------
for region in "${TARGET_REGIONS[@]}"; do
    echo "=============================="
    echo "Processing region: $region"
    echo "=============================="

    # ----------- COPY AMI -----------
    echo "Copying AMI $SOURCE_AMI_ID from $SOURCE_REGION to $region..."
    copied_ami_id=$(aws ec2 describe-images \
        --region "$region" \
        --filters "Name=name,Values=${SOURCE_AMI_ID}-copy-to-${region}" \
        --query "Images[0].ImageId" \
        --output text)

    if [[ "$copied_ami_id" != "None" ]]; then
        echo "AMI exists in $region: $copied_ami_id"
    else
            copied_ami_id=$(aws ec2 copy-image \
                --source-region "$SOURCE_REGION" \
                --source-image-id "$SOURCE_AMI_ID" \
                --name "${SOURCE_AMI_ID}-copy-to-${region}" \
                --region "$region" \
                --query "ImageId" \
                --output text)

            echo " → New AMI ID: $copied_ami_id"

            # Tag the copied AMI
            aws ec2 create-tags \
                --resources "$copied_ami_id" \
                --tags Key="$TAG_KEY",Value="$TAG_VALUE" \
                --region "$region"
            echo " → Tagged AMI"
    fi

    # ----------- CREATE SECURITY GROUP -----------
    vpc_id="${REGION_VPC[$region]}"

    # Check if SG already exists

    existing_sg_id=$(aws ec2 describe-security-groups \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$vpc_id" \
        --region "$region" \
        --query "SecurityGroups[0].GroupId" \
        --output text 2>/dev/null || echo "")

    if [[ -n "$existing_sg_id" && "$existing_sg_id" != "None" ]]; then
        echo " → Security Group $SECURITY_GROUP_NAME already exists ($existing_sg_id), deleting..."

        # Revoke all ingress rules
        ingress_rules=$(aws ec2 describe-security-groups \
            --group-ids "$existing_sg_id" \
            --region "$region" \
            --query "SecurityGroups[0].IpPermissions" --output json)
        if [[ "$ingress_rules" != "[]" ]]; then
            aws ec2 revoke-security-group-ingress \
                --group-id "$existing_sg_id" \
                --region "$region" \
                --ip-permissions "$ingress_rules"
        fi

        # Revoke all egress rules
        egress_rules=$(aws ec2 describe-security-groups \
            --group-ids "$existing_sg_id" \
            --region "$region" \
            --query "SecurityGroups[0].IpPermissionsEgress" --output json)
        if [[ "$egress_rules" != "[]" ]]; then
            aws ec2 revoke-security-group-egress \
                --group-id "$existing_sg_id" \
                --region "$region" \
                --ip-permissions "$egress_rules"
        fi

        # Delete the SG
        aws ec2 delete-security-group \
            --group-id "$existing_sg_id" \
            --region "$region"
        echo " → Deleted existing Security Group $existing_sg_id"
    fi

    echo "Creating security group $SECURITY_GROUP_NAME..."
    sg_id=$(aws ec2 create-security-group \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "Allow all inbound and outbound traffic" \
        --vpc-id "$vpc_id" \
        --region "$region" \
        --query 'GroupId' \
        --output text)

    # Allow all inbound
    aws ec2 authorize-security-group-ingress \
        --group-id "$sg_id" \
        --protocol -1 \
        --port all \
        --cidr 0.0.0.0/0 \
        --region "$region"

    echo " → Security Group ID: $sg_id created with all traffic allowed"

    # ----------- CREATE LAUNCH TEMPLATE -----------
    launch_template_name="${LAUNCH_TEMPLATE_NAME_PREFIX}-${region}"
    subnet_id="${REGION_SUBNET[$region]}"
    az="${REGION_AZ[$region]}"

    existing_lt_id=$(aws ec2 describe-launch-templates \
        --region "$region" \
        --filters "Name=launch-template-name,Values=$launch_template_name" \
        --query "LaunchTemplates[0].LaunchTemplateId" \
        --output text 2>/dev/null || echo "")


    if [[ -n "$existing_lt_id" && "$existing_lt_id" != "None" ]]; then
        echo "Launch Template $launch_template_name exists: $existing_lt_id, deleting..."
        aws ec2 delete-launch-template \
            --launch-template-id "$existing_lt_id" \
            --region "$region"
        echo " → Launch Template deleted."
    fi

    echo "Creating Launch Template $launch_template_name..."

    launch_template_id=$(aws ec2 create-launch-template \
        --launch-template-name "$launch_template_name" \
        --version-description "v1" \
        --launch-template-data "{
            \"ImageId\":\"$copied_ami_id\",
            \"KeyName\":\"$KEY_PAIR_NAME\",
            \"SecurityGroupIds\":[\"$sg_id\"],
              \"NetworkInterfaces\": [
                {
                  \"DeviceIndex\": 0,
                  \"SubnetId\": \"$subnet_id\",
                  \"Groups\": [\"$sg_id\"],
                  \"AssociatePublicIpAddress\": true
                }
              ],
            \"Placement\":{\"AvailabilityZone\":\"$az\"},
            \"BlockDeviceMappings\":[
                {
                    \"DeviceName\":\"/dev/xvda\",
                    \"Ebs\":{\"VolumeSize\":$ROOT_VOLUME_SIZE,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}
                }
            ]
        }" \
        --region "$region" \
        --query "LaunchTemplate.LaunchTemplateId" \
        --output text)

    echo " → Launch Template $launch_template_name created in $region with ID: $launch_template_id"

done

echo "All regions processed successfully!"
