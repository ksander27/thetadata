export NAME=dss
export IMAGE=dataiku/dss

# Run container
docker run --rm -d -it \
    -p 10000:10000 \
    -v dss_data:/home/dataiku/dss \
    --name $NAME \
    $IMAGE

# Access
docker exec -it $NAME /bin/bash