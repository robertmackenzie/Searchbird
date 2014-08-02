#!/bin/bash
echo "Starting searchbird in development mode..."
java -server -Xmx1024m -Dstage=development -jar ./dist/searchbird/@DIST_NAME@-@VERSION@.jar
