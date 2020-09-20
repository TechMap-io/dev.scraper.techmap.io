#!/bin/bash -l
cd /app

/usr/bin/java -Djdk.tls.client.protocols=TLSv1.2 -jar techmap-scraping-system.jar $@
