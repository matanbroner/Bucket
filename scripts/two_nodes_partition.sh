dir="$PWD/.."

docker stop node1 && docker rm node1
docker stop node2 && docker rm node2

# ignores if network exists
docker network create --subnet=10.10.0.0/16 kv_subnet || true

docker build -t kvs:3.0 $dir

docker run -d -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" \
      -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800" -e REPL_FACTOR=1 \
      kvs:3.0

docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" \
      -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.5:13800" -e REPL_FACTOR=1 \
      kvs:3.0

sleep 3

# init keys a and b in both nodes
curl --request   PUT \
       --header    "Content-Type: application/json" \
       --write-out "" \
       --data      '{"value": "init_a"}' \
       http://127.0.0.1:13801/kvs/keys/a

curl --request   PUT \
       --header    "Content-Type: application/json" \
       --write-out "" \
       --data      '{"value": "init_a"}' \
       http://127.0.0.1:13802/kvs/keys/a

curl --request   PUT \
       --header    "Content-Type: application/json" \
       --write-out "" \
       --data      '{"value": "init_b"}' \
       http://127.0.0.1:13801/kvs/keys/b

curl --request   PUT \
       --header    "Content-Type: application/json" \
       --write-out "" \
       --data      '{"value": "init_b"}' \
       http://127.0.0.1:13802/kvs/keys/b