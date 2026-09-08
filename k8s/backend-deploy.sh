#!/bin/bash
# Redeploys the backend on the remote k8s node: deletes the existing
# Deployment+Service, replaces the containerd image with the freshly-built
# tar, then re-applies every manifest in this directory (k8s/).
#
# Run FROM the directory this script + backend-deployment.yaml were copied
# into on the remote host (BASE_DIR below) — Jenkins copies both there before
# invoking this script (see Jenkinsfile's "Deploy Backend on Remote Server"
# stage).
set -e
export KUBECONFIG=/etc/kubernetes/admin.conf

# ===============================
# CONFIG (root-safe absolute paths)
# ===============================
BASE_DIR="/home/rcv/daas_installer/daas_v1/devraq-backend"
TAR_FILE=$(ls -t /home/rcv/daas_installer/daas_tar/devraq-backend_*.tar | head -n1)
NAMESPACE="thinkcloud"
BACKEND_YAML="backend-deployment.yaml"
NEW_IMAGE_TAG="1.1.1"
IMAGE="docker.io/library/devraq-backend:${NEW_IMAGE_TAG}"

echo "👤 Running as user: $(whoami)"
echo "📍 Current dir: $(pwd)"

# ===============================
# PATH VALIDATION
# ===============================
echo "📁 Checking BASE_DIR: ${BASE_DIR}"
if [ ! -d "$BASE_DIR" ]; then
  echo "❌ ERROR: Directory does not exist: $BASE_DIR"
  exit 1
fi

echo "📦 Checking TAR file: ${TAR_FILE}"
if [ ! -f "$TAR_FILE" ]; then
  echo "❌ ERROR: Tar file not found: $TAR_FILE"
  exit 1
fi

cd "$BASE_DIR"
pwd

# ===============================
# DELETE DEPLOYMENT
# ===============================
echo "=============================="
echo "1️⃣ Deleting backend deployment & service"
echo "=============================="

kubectl delete -f "$BACKEND_YAML" -n "$NAMESPACE" --ignore-not-found=true

sleep 5

# ===============================
# REMOVE OLD IMAGE (optional but clean)
# ===============================
echo "=============================="
echo "2️⃣ Removing old backend image (containerd)"
echo "=============================="

sudo ctr -n k8s.io images rm "$IMAGE" || echo "ℹ️ Image already removed"

# ===============================
# LOAD IMAGE FROM TAR
# ===============================
echo "=============================="
echo "3️⃣ Loading backend image from TAR"
echo "=============================="

sudo ctr -n k8s.io images import "$TAR_FILE"

# ===============================
# APPLY K8s YAMLs
# ===============================
echo "=============================="
echo "4️⃣ Applying Kubernetes manifests"
echo "=============================="

kubectl apply -f . -n "$NAMESPACE"

echo "=============================="
echo "✅ Backend redeploy completed successfully"
echo "=============================="
