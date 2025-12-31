#!/bin/bash

# Check if the user provided an argument
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <user> <dev> <master|slave|none>"
    exit 1
fi

USER=$1
INTERFACE=$2
ROLE=$3

echo "Using interface: $INTERFACE"

# Add dockergroup permission
usermod -aG docker $USER
echo "Docker Permissions"

# Function to kill any running ptp4l and phc2sys processes
kill_running_processes() {
    # Kill ptp4l process
    echo "Checking for running ptp4l processes..."
    pkill -f ptp4l

    # Kill phc2sys process
    echo "Checking for running phc2sys processes..."
    pkill -f phc2sys
}

kill_running_processes

if [ "$ROLE" = "none" ]; then
  echo "Keeping NTP"
  sudo systemctl enable ntp
  sudo systemctl restart ntp
  exit 0
fi

# Stop NTP service
sudo systemctl stop ntp
sudo systemctl disable ntp
service ntp stop
echo "NTP Stopped"




# Activate PTP according to the selected role
if [ "$ROLE" = "master" ]; then
    echo "Starting PTP master on $INTERFACE..."
    ptp4l -i "$INTERFACE" -m -H > /tmp/ptp_master.log 2>&1 &
    sleep 2  # Give ptp4l some time to establish sync
    echo "Starting clock synchronization on master..."
    phc2sys -s "$INTERFACE" -c CLOCK_REALTIME -w -m > /tmp/phc_master.log 2>&1 &
    echo "PTP master started."
elif [ "$ROLE" = "slave" ]; then
    echo "Starting PTP slave on $INTERFACE..."
    ptp4l -i "$INTERFACE" -m -s -H > /tmp/ptp_slave.log 2>&1 &
    sleep 2  # Give ptp4l some time to establish sync
    echo "Starting clock synchronization..."
    phc2sys -s "$INTERFACE" -c CLOCK_REALTIME -w -m > /tmp/phc_slave.log 2>&1 &
    echo "PTP slave synchronization started."
else
    echo "Invalid role. Use 'master' or 'slave'."
    exit 1
fi

# Resetting connection
#newgrp docker
