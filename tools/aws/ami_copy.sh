#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# CONFIGURATION
# -----------------------------
SOURCE_REGION="us-east-1"              # Region where the original AMI exists
SOURCE_AMI_ID="ami-0d597e5e05169820a"   # Original AMI ID
TARGET_REGIONS=("us-east-1" "us-east-2" "eu-west-1" "eu-west-2" "ap-northeast-1" "ap-northeast-2" "ap-southeast-1" "ap-southeast-2")  # <-- replace with your regions

TAG_KEY="CopiedFrom"
TAG_VALUE="${SOURCE_AMI_ID}"

TEMPLATES=(
  "us-east-1|lt-05fce66f6e6417d52"
  "us-east-2|lt-0bb7871135ffaff60"
  "eu-west-1|lt-0bedaa5ac6a3f3079"
  "eu-west-2|lt-0ca585dc4d4f77343"
  "ap-northeast-1|lt-00d86f743930e63bb"
  "ap-northeast-2|lt-0316e8cb2b9a90093"
  "ap-southeast-1|lt-0e5335e50589c44eb"
  "ap-southeast-2|lt-0049f583b7cb28dd8"
)

# ============================================================
# INTERNAL ARRAYS
# ============================================================
declare -A REGION_TEMPLATE
declare -A REGION_NEW_TEMPLATES
# ============================================================
# PARSE THE CONFIG MAP
# ============================================================

echo "Loading Template ids..."
for entry in "${TEMPLATES[@]}"; do
  region=$(echo "$entry" | cut -d "|" -f1)
  template=$(echo "$entry"   | cut -d "|" -f2)

  REGION_TEMPLATE["$region"]="$template"
done
echo

# ============================================================
# CREATE NEW IMAGE AND UPDATE TEMPLATE
# ============================================================

for region in "${TARGET_REGIONS[@]}"; do
    echo "=============================="
    echo "Processing region: $region"
    echo "=============================="

    # ----------- COPY AMI -----------
    echo "Copying AMI $SOURCE_AMI_ID from $SOURCE_REGION to $region..."

    if [[ "$region" == "$SOURCE_REGION" ]]; then
      copied_ami_id=$SOURCE_AMI_ID
    else
      copied_ami_id=$(aws ec2 describe-images \
          --region "$region" \
          --filters "Name=name,Values=${SOURCE_AMI_ID}-copy-to-${region}" \
          --query "Images[0].ImageId" \
          --output text)
    fi

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

    lt="${REGION_TEMPLATE[$region]}"
    echo "Updating Template $lt of region $region"

    LATEST_VERSION=$(aws ec2 describe-launch-templates \
      --region "$region" \
      --launch-template-ids "$lt" \
      --query "LaunchTemplates[0].LatestVersionNumber" \
      --output text)

    if [[ "$LATEST_VERSION" == "None" ]]; then
                echo "  ✗ Unable to get latest version for $lt"
                continue
    fi

    # Get the full LaunchTemplateData JSON for the latest version
    TEMPLATE_DATA=$(aws ec2 describe-launch-template-versions \
        --region "$region" \
        --launch-template-id "$lt" \
        --versions "$LATEST_VERSION" \
        --query "LaunchTemplateVersions[0].LaunchTemplateData")

    # Extract the template's current AMI
    CURRENT_AMI=$(echo "$TEMPLATE_DATA" | jq -r '.ImageId')

    if [[ "$CURRENT_AMI" == "$copied_ami_id" ]]; then
      echo "Template already updated, skipping..."
      REGION_NEW_TEMPLATES["$region"]="$LATEST_VERSION"
      continue
    fi


    # Replace AMI ID
    UPDATED_TEMPLATE_DATA=$(echo "$TEMPLATE_DATA" | jq --arg AMI "$copied_ami_id" '.ImageId = $AMI')

    # Create the new Launch Template version
    NEW_VERSION=$(aws ec2 create-launch-template-version \
        --region "$region" \
        --launch-template-id "$lt" \
        --source-version "$LATEST_VERSION" \
        --launch-template-data "$UPDATED_TEMPLATE_DATA" \
        --query "LaunchTemplateVersion.VersionNumber" \
        --output text)

    if [[ $? -eq 0 ]]; then
        echo "  New launch template version created: $NEW_VERSION"
        REGION_NEW_TEMPLATES["$region"]="$NEW_VERSION"
    else
        echo "  Failed to create new version for $lt"
    fi
done

for region in "${TARGET_REGIONS[@]}"; do
  template_version=$REGION_NEW_TEMPLATES["$region"]
  echo "Region $region : Template Version $template_version"
done

