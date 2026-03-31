open -a Docker

docker run -p 9010:9010 gcr.io/cloud-spanner-emulator/emulator

export SPANNER_EMULATOR_HOST=localhost:9010
