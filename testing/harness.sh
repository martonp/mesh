#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROOT_DIR=~/.tatanka-test
TATANKA_DIR="$SCRIPT_DIR/../cmd/tatanka"
TATANKA_BIN=$TATANKA_DIR/tatanka
TATANKACTL_DIR="$SCRIPT_DIR/../cmd/tatankactl"
TATANKACTL_BIN=$TATANKACTL_DIR/tatankactl
TESTCLIENT_DIR="$SCRIPT_DIR/../cmd/testclient"
TESTCLIENT_BIN=$TESTCLIENT_DIR/testclient
TESTCLIENT_UI_DIR="$SCRIPT_DIR/client/ui"

build_tatanka() {
  cd "$TATANKA_DIR"
  go build -o tatanka
  cd - > /dev/null
}

build_tatankactl() {
  cd "$TATANKACTL_DIR"
  go build -o tatankactl
  cd - > /dev/null
}

build_testclient() {
  cd "$TESTCLIENT_DIR"
  go build -o testclient
  cd - > /dev/null
}

build_testclient_ui() {
  cd "$TESTCLIENT_UI_DIR"
  if [ ! -d node_modules ]; then
    npm install
  fi
  npm run build
  cd - > /dev/null
}

create_config() {
  local node_dir=$1
  local listen_port=$2
  local metrics_port=$3
  local admin_port=$4
  local bootstrap_list_port=$5
  local config_path=$node_dir/tatanka.conf

  cat <<EOF > $config_path
appdata=$node_dir
listenport=$listen_port
metricsport=$metrics_port
adminport=$admin_port
bootstraplistport=$bootstrap_list_port
EOF

  echo $config_path
}

create_client_config() {
  local client_dir=$1
  local node_addr=$2
  local client_port=$3
  local web_port=$4
  local config_path=$client_dir/testclient.conf

  cat <<EOF > $config_path
appdata=$client_dir
loglevel=debug
nodeaddr=$node_addr
clientport=$client_port
webport=$web_port
EOF

  echo $config_path
}

start_harness() {
  local num_nodes=$1
  local num_clients=$2

  mkdir -p $ROOT_DIR

  build_tatanka
  build_tatankactl
  build_testclient
  build_testclient_ui

  # Store peer IDs, listen ports, admin ports, and bootstrap list ports for each node.
  node_peer_ids=()
  node_listen_ports=()
  node_admin_ports=()
  node_bootstrap_list_ports=()

  # Initialize nodes and generate private keys using tatanka init.
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    mkdir -p $node_dir

    peer_id=$("$TATANKA_BIN" init --appdata "$node_dir")
    node_peer_ids+=("$peer_id")

    listen_port=$((12345 + i))
    metrics_port=$((12355 + i))
    admin_port=$((12365 + i))
    bootstrap_list_port=$((12375 + i))
    node_listen_ports+=("$listen_port")
    node_admin_ports+=("$admin_port")
    node_bootstrap_list_ports+=("$bootstrap_list_port")
    config_path=$(create_config $node_dir $listen_port $metrics_port $admin_port $bootstrap_list_port)
  done

  # Write shared whitelist into each node's data dir.
  whitelist_peers=()
  for i in $(seq 0 $((num_nodes - 1))); do
    whitelist_peers+=("\"${node_peer_ids[$i]}\"")
  done
  whitelist_json=$(printf ",%s" "${whitelist_peers[@]}")
  whitelist_json="${whitelist_json:1}"  # Remove leading comma
  whitelist="[$whitelist_json]"

  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    echo "$whitelist" > $node_dir/whitelist.json
  done

  # Start tmux session with interleaved node/ctl windows starting at 0
  # so that 5 nodes fit in windows 0-9 (node-1=0, ctl-1=1, node-2=2, ...).
  session_name="tatanka-test"
  tmux new-session -d -s $session_name -x 200 -y 50
  win=0
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    config_path=$node_dir/tatanka.conf

    # Build bootstrap flags for all OTHER nodes.
    bootstrap_flags=""
    for j in $(seq 0 $((num_nodes - 1))); do
      if [ $j -ne $((i - 1)) ]; then
        bootstrap_flags="$bootstrap_flags --bootstrap /ip4/127.0.0.1/tcp/${node_listen_ports[$j]}/p2p/${node_peer_ids[$j]}"
      fi
    done

    # Node window
    if [ $win -eq 0 ]; then
      tmux rename-window -t $session_name:0 node-$i
    else
      tmux new-window -t $session_name:$win -n node-$i
    fi
    tmux send-keys -t $session_name:$win "$TATANKA_BIN -C $config_path$bootstrap_flags" C-m
    win=$((win + 1))

    # Ctl window
    admin_port=${node_admin_ports[$((i - 1))]}
    tmux new-window -t $session_name:$win -n ctl-$i
    tmux send-keys -t $session_name:$win "$TATANKACTL_BIN -a localhost:$admin_port" C-m
    win=$((win + 1))
  done

  sleep 2 # wait for nodes to start before starting clients

  # Start test clients and evenly distribute them across the nodes.
  if [ "$num_clients" -gt 0 ]; then
    for i in $(seq 1 $num_clients); do
      client_dir=$ROOT_DIR/client-$i
      mkdir -p $client_dir

      node_idx=$(( (i - 1) % num_nodes ))
      node_peer_id=${node_peer_ids[$node_idx]}
      node_listen_port=${node_listen_ports[$node_idx]}
      node_addr="/ip4/127.0.0.1/tcp/$node_listen_port/p2p/$node_peer_id"

      client_port=$((12455 + i))
      web_port=$((12465 + i))
      client_cfg=$(create_client_config $client_dir "$node_addr" $client_port $web_port)

      tmux new-window -t $session_name -n client-$i
      tmux send-keys -t $session_name "ROOT_DIR=$ROOT_DIR $TESTCLIENT_BIN -C $client_cfg" C-m
    done
  fi

  echo "Started $num_nodes nodes and $num_clients clients in tmux session '$session_name'"
  for i in $(seq 1 $num_nodes); do
    bootstrap_list_port=${node_bootstrap_list_ports[$((i - 1))]}
    echo "  node-$i bootstrap list: http://127.0.0.1:$bootstrap_list_port/bootstrap"
  done
}

# Check if number of nodes and clients are provided
if [ $# -lt 2 ]; then
  echo "Usage: $0 <num_nodes> <num_clients>"
  echo "  num_nodes:   Number of tatanka nodes to start"
  echo "  num_clients: Number of test clients to start"
  exit 1
fi

num_nodes=$1
num_clients=$2

# Validate that the argument is a positive integer
if ! [[ "$num_nodes" =~ ^[0-9]+$ ]] || [ "$num_nodes" -eq 0 ]; then
  echo "Error: num_nodes must be a positive integer"
  exit 1
fi

if ! [[ "$num_clients" =~ ^[0-9]+$ ]] || [ "$num_clients" -lt 0 ]; then
  echo "Error: num_clients must be a non-negative integer"
  exit 1
fi

start_harness $num_nodes $num_clients

tmux attach-session -t $session_name
