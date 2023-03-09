export NAME=postgres

docker run \
    --name $NAME  \
    -v postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=alex2027 \
    -d \
    postgres

docker exec -it $NAME psql -U postgres


