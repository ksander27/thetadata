export JUPDIR=/home/jupyter
export USR=alex@hubert.fyi
export PWD=alex2027
export NAME=web
export IMAGE=webapp:latest
export VERSION=unstable

# Stop existing container
docker stop $NAME 
# Build image 
docker image build -t $IMAGE .
# push image
#docker push $IMAGE

# Run container
docker run --rm -d -it \
    -p 8888:8888 \
    -v thetadata:$JUPDIR \
    -e USERNAME=$USR \
    -e PASSWORD=$PWD \
    -e VERSION=$VERSION \
    --name $NAME \
    $IMAGE

# Access
docker exec -it $NAME /bin/bash