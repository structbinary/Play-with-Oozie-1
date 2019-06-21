#!/bin/bash

sudo chmod 755 /var/run/cloudera-scm-agent/process/

export PATH="/home/cdhadmin/anaconda2/bin:$PATH"

export HUE_CONF_DIR="/var/run/cloudera-scm-agent/process/`ls -alrt /var/run/cloudera-scm-agent/process | grep -i HUE_SERVER | tail -1 | awk '{print $9}'`"

sudo chmod -R 757 $HUE_CONF_DIR

HUE_IGNORE_PASSWORD_SCRIPT_ERRORS=1 HUE_DATABASE_PASSWORD=P8T0wc1L6s /opt/cloudera/parcels/CDH/lib/hue/build/env/bin/hue loaddata $1