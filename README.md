🇰🇷 [한국어](README_ko.md) | 🇺🇸 English

# Apache Druid 37.0.0 — Kubernetes Kustomize Deployment Guide

> Apache Druid 37.0.0 Kubernetes configuration deployed directly via StatefulSets without an Operator.  
> Structured with the Kustomize Components pattern so storage backends can be swapped as discrete modules.

---

## Table of Contents

1. [Infrastructure](#1-infrastructure)
2. [Full Kustomize Structure](#2-full-kustomize-structure)
3. [Usage (Quick Start)](#3-usage-quick-start)
4. [File Descriptions](#4-file-descriptions)
5. [Storage Component Selection](#5-storage-component-selection)
6. [Key Design Decisions](#6-key-design-decisions)
7. [Known Issues](#7-known-issues) (7-5: Jetty 12 Router thread shortage included)
8. [Troubleshooting Guide](#8-troubleshooting-guide)
9. [Kafka Configuration Reference](#9-kafka-configuration-reference)
10. [Druid Architecture — Component Roles and Data Flow](#10-druid-architecture--component-roles-and-data-flow)

---

## 1. Infrastructure

### Druid Component Layout

| Component | Port | replicas (default) | Role |
|-----------|------|-------------------|------|
| Coordinator | 8081 | 1 | Segment placement and load management |
| Overlord | 8090 | 1 | Task scheduling and worker management |
| Broker | 8082 | 1 | Query routing and result merging |
| Historical | 8083 | 1 | Segment loading and query processing |
| Indexer | 8091 | 1 | Kafka ingest + Task execution (JVM thread model) |
| Router | 8888 | 1 | UI / API Gateway |

> Setting Indexer to 2 replicas creates an HA configuration. See [6-3. Indexer HA Setup](#6-3-indexer-ha-setup-replicas2) for details.

---

## 2. Full Kustomize Structure

```
apache-druid-kustomize/
│
├── base/                              # Base configuration (local storage + emptyDir)
│   ├── kustomization.yaml             # Resource list
│   ├── rbac.yaml                      # ServiceAccount, Role, RoleBinding
│   ├── postgres-secret.yaml           # PostgreSQL Secret (defaults)
│   ├── postgres.yaml                  # PostgreSQL Service + StatefulSet
│   ├── common-configmap.yaml          # Druid common settings (local storage)
│   ├── {component}-configmap.yaml     # Per-component JVM / log4j2 / runtime.properties
│   └── {component}.yaml              # StatefulSet + Service (uses emptyDir)
│
├── components/                        # Reusable feature modules
│   ├── pvc/                           # Replace emptyDir with PVC
│   │   └── kustomization.yaml         # JSON 6902 patches (all 7 StatefulSets)
│   └── storage/                       # Storage backend selection
│       ├── s3/                        # S3 / MinIO (AWS S3 compatible)
│       ├── azure/                     # Microsoft Azure Blob Storage
│       ├── gcs/                       # Google Cloud Storage
│       ├── hdfs/                      # Apache HDFS
│       ├── aliyun-oss/                # Alibaba Cloud OSS (community)
│       ├── cloudfiles/                # Rackspace Cloud Files (community)
│       └── cassandra/                 # Apache Cassandra (community, caution)
│
└── overlays/
    ├── apache-druid-37.0.0/           # Reference overlay — committed to git
    │   ├── kustomization.yaml         # Component selection + patch references
    │   ├── postgres.env               # PostgreSQL credentials (.gitignored)
    │   ├── postgres.env.example       # Credentials template (tracked in git)
    │   └── patches/
    │       ├── replicas.yaml          # Indexer replicas = 2
    │       ├── jvm-heaps.yaml         # Per-component JVM heap sizes
    │       ├── k8s-resources.yaml     # K8s requests / limits
    │       ├── pvc-patch.yaml         # PVC sizes (storageClassName override)
    │       ├── druid-common-config-with-minio.yaml    # ← active storage backend
    │       ├── druid-common-config-with-aws-s3.yaml   # (choose one)
    │       ├── druid-common-config-with-azure.yaml
    │       ├── druid-common-config-with-gcs.yaml
    │       ├── druid-common-config-with-hdfs.yaml
    │       └── statefulsets-with-aws-region.yaml      # S3/MinIO: AWS_REGION injection
    └── <your-cluster>/                # Your custom overlay — gitignored
        └── (copy of apache-druid-37.0.0, edit as needed)
```

### Design Principles

```
base/        Minimal working configuration (emptyDir, local storage).
             Do NOT modify — treated as a read-only reference.

components/  Optional reusable modules (PVC, storage backends).
             Do NOT modify — compose by referencing from overlays.

overlays/    Environment-specific final configuration.
             Copy the reference overlay and edit only what differs.
```

**Git policy**: `base/` and `components/` are committed as-is. `overlays/apache-druid-37.0.0/` is the committed reference. All other overlay directories are gitignored — manage them outside git or with a secrets tool.

---

## 3. Usage (Quick Start)

### Create Your Overlay

`overlays/apache-druid-37.0.0/` is the reference overlay committed to git.  
For your own environment, copy it and edit only what you need — never modify base/ or components/.

```bash
# Copy the reference overlay
cp -rp overlays/apache-druid-37.0.0 overlays/<your-cluster>

# Create postgres.env from the example
cp overlays/<your-cluster>/postgres.env.example \
   overlays/<your-cluster>/postgres.env
vi overlays/<your-cluster>/postgres.env

# Edit kustomization.yaml: choose storage backend, enable/disable PVC, etc.
vi overlays/<your-cluster>/kustomization.yaml

# Fill in credentials in the active storage patch
vi overlays/<your-cluster>/patches/druid-common-config-with-minio.yaml
```

### Deploy

```bash
# Deploy your overlay
kubectl apply -k overlays/<your-cluster>/

# Deploy base only (local storage + emptyDir, for dev/test)
kubectl apply -k base/

# Dry-run preview
kubectl kustomize overlays/<your-cluster>/ | kubectl apply --dry-run=client -f -
```

### Verify Deployment

```bash
kubectl get pod -o wide
kubectl get pvc
kubectl get svc
```

### Accessing the Druid UI

To allow external access to the Router, enable the `components/router-external` component.  
This component creates a `type: LoadBalancer` service (`druid-router-external`).

```yaml
# overlays/apache-druid-37.0.0/kustomization.yaml
components:
  - ../../components/router-external   # Creates a LoadBalancer service when enabled
```

After applying, check the External IP:

```bash
kubectl get svc druid-router-external
# NAME                    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)
# druid-router-external   LoadBalancer   10.96.x.x      <YOUR-LB-IP>     80:xxxxx/TCP
```

Once the LoadBalancer IP is assigned, open `http://<EXTERNAL-IP>` in a browser.

For local access without an External IP, use port-forward:

```bash
kubectl port-forward svc/druid-routers 8888:8888
# Access at http://localhost:8888
```

### Druid Version Upgrade

The overlay directory name and `images.newTag` in `kustomization.yaml` jointly manage the version.  
The StatefulSets in base declare `image: apache/druid` (no tag), and the overlay injects the tag.

```bash
# Create a new version overlay (e.g., 38.0.0)
cp -r overlays/apache-druid-37.0.0 overlays/apache-druid-38.0.0

# In overlays/apache-druid-38.0.0/kustomization.yaml, change only one line
images:
  - name: apache/druid
    newTag: "38.0.0"   ← change only this

# Deploy
kubectl apply -k overlays/apache-druid-38.0.0/
```

### Applying Updates

```bash
# After configuration changes
kubectl apply -k overlays/apache-druid-37.0.0/

# Restart a specific component
kubectl rollout restart statefulset/druid-indexers
```

---

## 4. File Descriptions

### `base/common-configmap.yaml`

Shared settings for all Druid components (`common.runtime.properties`).

| Setting Key | Default | Description |
|-------------|---------|-------------|
| `druid.discovery.type` | `k8s` | Service discovery via Kubernetes API without ZooKeeper |
| `druid.discovery.k8s.clusterIdentifier` | `druid-default` | Cluster identifier (unique per namespace) |
| `druid.storage.type` | `local` | Base default; replaced by overlay with s3/azure/gcs etc. |
| `druid.metadata.storage.type` | `postgresql` | Metadata DB type |
| `druid.emitter` | `prometheus` | Metrics collection method |

### `base/{component}-configmap.yaml`

Per-component ConfigMap. Contains three file keys:

```
runtime.properties  → per-component port, role, processing settings
jvm.config          → JVM heap, GC, direct memory settings
log4j2.xml          → log output configuration (Indexer includes RoutingAppender)
```

### `base/indexer-configmap.yaml` — Special Configuration

The Indexer's `log4j2.xml` includes a **RoutingAppender**.  
When the MDC key `task.log.id` is set during task execution, logs are automatically written to a file.

```xml
<Routing name="TaskRouting">
  <Routes pattern="${{ctx:task.log.id}}">
    <Route key="">          <!-- not a task thread → discard -->
      <Null name="Null"/>
    </Route>
    <Route>                 <!-- task thread → write to file -->
      <File fileName="${{ctx:task.log.file}}" .../>
    </Route>
  </Routes>
</Routing>
```

This allows the **Tasks → Logs** button in the Druid UI to function correctly.

### `base/{component}.yaml`

StatefulSet + Service definitions. Base uses `emptyDir` for all:

```yaml
volumes:
  - name: common-config
    configMap:
      name: druid-common-config
  - name: node-config
    configMap:
      name: druid-{component}-config
  - name: data
    emptyDir: {}            # replaced with PVC by the pvc component in overlay
```

#### Pod Environment Variables — POD_NAME / POD_NAMESPACE / POD_IP

All Druid StatefulSets share these common env vars:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name        # e.g.: druid-brokers-0
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace   # e.g.: default
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP         # e.g.: 10.233.102.5
  - name: druid_host
    value: "$(POD_IP)"                  # used by Druid as its own address
```

**Role and reason for each variable:**

| Variable | Location | Purpose | Problem when absent |
|----------|----------|---------|---------------------|
| `POD_NAME` | base | Used by `druid-kubernetes-extensions` as the Pod name when patching its own Pod label | Label patch fails → Overlord fails to recognize the worker |
| `POD_NAMESPACE` | base | Specifies the namespace when calling the Kubernetes API | Cross-namespace misbehavior |
| `POD_IP` | base | Injected as the `druid_host` value — used by Druid to advertise its cluster address | Falls back to hostname → inter-component communication fails |
| `druid_host` | base | Overrides the `druid.host` setting via env | — |
| `AWS_REGION` | **S3 component only** | Used by the AWS SDK to resolve the S3 endpoint region | S3/MinIO connection may fail |

**Why `druid_host=$(POD_IP)` is required:**

In Druid 37.0.0, if `druid.host` is not set explicitly, the JVM returns the hostname via `InetAddress.getLocalHost().getHostName()`. A Kubernetes Pod's hostname (`druid-brokers-0`) cannot be resolved by other Pods without DNS. Using the Pod IP directly ensures that inter-component HTTP communication works correctly.

```
With hostname: druid-brokers-0 → DNS lookup fails or returns wrong IP → communication fails
With POD_IP:   10.233.102.5    → direct communication works ✅
```

> This issue was discovered during a Druid 37.0.0 upgrade when all inter-pod communication failed.

**Why `AWS_REGION` is in the S3 component only, not in base:**

Environments that don't use S3/MinIO (local emptyDir, Azure, GCS, etc.) don't need `AWS_REGION`.  
When `components/storage/s3` is enabled, Kustomize automatically injects it into all StatefulSets.

```
# components/storage/s3/statefulset-env-patch.yaml
- op: add
  path: /spec/template/spec/containers/0/env/-
  value:
    name: AWS_REGION
    value: "us-east-1"   # Any value works for MinIO / set to your bucket region for AWS S3
```

The `AWS_REGION` value is irrelevant for MinIO (the endpoint is set explicitly via `druid.s3.endpoint.url`).  
For actual AWS S3, change this to the bucket's region (e.g., `ap-northeast-2`).

### `components/pvc/kustomization.yaml`

Removes `emptyDir` and adds `volumeClaimTemplates` via JSON 6902 patches.  
No `storageClassName` is set — the cluster default StorageClass is used automatically.

```
PostgreSQL  → 8Gi  (metadata DB, PVC strongly recommended)
Coordinator → 5Gi
Overlord    → 5Gi
Broker      → 10Gi
Historical  → 10Gi (segment cache)
Indexer     → 10Gi (task temp directory)
Router      → 5Gi
```

To override StorageClass or sizes without touching the component, use the overlay patch:

```yaml
# overlays/apache-druid-37.0.0/patches/pvc-patch.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-historicals
spec:
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        storageClassName: longhorn   # override cluster default
        resources:
          requests:
            storage: 50Gi           # override size
```

### `components/storage/{backend}/patch.yaml`

Replaces the entire `common.runtime.properties` key in `druid-common-config`.  
Contains the extensions and settings appropriate for each storage backend.

### `overlays/apache-druid-37.0.0/patches/replicas.yaml`

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-indexers
spec:
  replicas: 2    # HA setup: each pod independently consumes all Kafka partitions
```

### `overlays/apache-druid-37.0.0/patches/jvm-heaps.yaml`

Replaces the `jvm.config` key for each component.

| Component | `-Xmx` | `-XX:MaxDirectMemorySize` | Notes |
|-----------|--------|--------------------------|-------|
| Coordinator | 256m | 256m | Metadata-focused, sufficient |
| Overlord | 256m | 256m | Task scheduling-focused |
| Broker | 512m | 256m | Consider increasing for complex queries |
| Historical | 512m | 256m | Scale with segment count |
| Indexer | **2g** | 512m | Scale with concurrent task count (worker.capacity) |
| Router | 256m | 128m | UI + routing-focused |

#### `-Djava.io.tmpdir=var/tmp` — Volume Relationship

This option, present in all component `jvm.config` files, is directly tied to the volume mount structure.

```
Druid process working directory: /opt/druid/
-Djava.io.tmpdir=var/tmp (relative path)
              └→ actual path: /opt/druid/var/tmp
```

Volume mount structure correspondence:

```
volume: data  (emptyDir or PVC)
  mountPath: /opt/druid/var              ← volume root
             /opt/druid/var/tmp          ← tmpdir  ★ here
             /opt/druid/var/druid/segments    ← segment cache (Historical)
             /opt/druid/var/druid/task/       ← task temp files (Indexer)
             /opt/druid/var/druid/indexing-logs/ ← task log files
```

Why tmpdir points inside the volume:

| Reason | Explanation |
|--------|-------------|
| Container root fs capacity limit | Default `/tmp` consumes ephemeral storage quota → disk exhaustion during large task execution |
| Task temp file size | Indexer creates hundreds of MB to several GB of temp files while building segments |
| Consistency with PVC | When `components/pvc` is applied, PVC covers `/opt/druid/var`, so tmpdir automatically uses PVC space |

> When switching base(emptyDir) → PVC, this option does not need to be changed.  
> Since `mountPath: /opt/druid/var` remains the same, tmpdir automatically points to the PVC.

#### `processing.*` Settings and `-XX:MaxDirectMemorySize` Coupling

`druid.processing.*` settings **directly consume Direct Memory (off-heap)**.  
Setting `MaxDirectMemorySize` smaller than this consumption causes `OutOfMemoryError: Direct buffer memory`.

```
Actual direct memory usage = (numThreads + numMergeBuffers) × sizeBytes + Netty IO overhead (~50~80MB)
```

| Component | `sizeBytes` | `numThreads` | `numMergeBuffers` | Processing Direct | Netty IO | Total | `MaxDirectMemorySize` | Headroom |
|-----------|------------|-------------|------------------|------------------|---------|-------|-----------------------|----------|
| Broker | 50MB | 1 | 2 | (1+2)×50 = 150MB | ~50MB | ~200MB | 256m | ~56MB ✅ |
| Historical | 50MB | 1 | 2 | (1+2)×50 = 150MB | ~50MB | ~200MB | 256m | ~56MB ✅ |
| Indexer | 100MB | 2 | 2 | (2+2)×100 = 400MB | ~80MB | ~480MB | 512m | ~32MB ⚠️ |

> **Indexer caution**: only ~32MB headroom.  
> When increasing `numThreads` or `sizeBytes`, you must raise `MaxDirectMemorySize` together.  
> With `worker.capacity=3`, concurrent tasks may consume additional buffers per task.

**Recalculation example when changing settings** (increasing numThreads from 2→4):
```
(4+2) × 100MB = 600MB + ~80MB ≈ 680MB
→ Raise MaxDirectMemorySize from 512m → 768m or higher
→ Also recalculate limits memory in k8s-resources.yaml
```

### `overlays/apache-druid-37.0.0/patches/k8s-resources.yaml`

The container memory `limits` must accommodate the **total JVM memory usage**.  
If `limits` is too low, the Linux OOM Killer forcibly terminates the process (`OOMKilled`).

> `-XX:+ExitOnOutOfMemoryError` only handles JVM heap OOM.  
> It cannot prevent container-level OOM Killer.

**Memory limit formula**:
```
limits >= Xmx + MaxDirectMemorySize + JVM overhead
                                      (metaspace, code cache,
                                       thread stack, GC overhead)
                                       ≈ 200~350Mi
```

**Per-component calculation and current status**:

| Component | Xmx | MaxDirect | overhead | Required | limit | Status |
|-----------|-----|-----------|---------|---------|-------|--------|
| Coordinator | 256m | 256m | ~300Mi | ~812Mi | 1Gi | ✅ headroom 212Mi |
| Overlord | 256m | 256m | ~300Mi | ~812Mi | 1Gi | ✅ headroom 212Mi |
| Broker | 512m | 256m | ~300Mi | ~1068Mi | 1Gi | ⚠️ barely fits (-44Mi) |
| Historical | 512m | 256m | ~300Mi | ~1068Mi | 1536Mi | ✅ headroom 468Mi |
| **Indexer** | **2048m** | **512m** | ~300Mi | **~2860Mi** | **2Gi** | ⚠️ **exceeds formula (-812Mi)** |
| Router | 256m | 128m | ~300Mi | ~684Mi | 512Mi | ⚠️ exceeds formula (-172Mi) |

> **Broker / Router**: OOMKilled risk under high query load or stress testing.  
> Recommended: Broker → `1536Mi`, Router → `768Mi` or higher.
>
> **Indexer**: `Xmx(2048Mi) + MaxDirect(512Mi) = 2560Mi` already exceeds limit(2Gi=2048Mi).  
> Operable because heap and direct memory rarely peak simultaneously, but  
> OOMKilled risk exists when increasing `worker.capacity` or processing large segments.  
> For safe operation, raise limit to `3Gi` or above (need 6Gi per node if replicas=2).

**How to detect OOMKilled**:
```bash
# Check termination reason for restarted pods
kubectl describe pod <pod-name> | grep -A5 "Last State"

# Search all pods for OOMKilled history
kubectl get pod -o json | python3 -c "
import json, sys
for p in json.load(sys.stdin)['items']:
  for c in p.get('status',{}).get('containerStatuses',[]):
    t = c.get('lastState',{}).get('terminated',{})
    if t.get('reason') == 'OOMKilled':
      print(p['metadata']['name'], c['name'], t.get('finishedAt',''))"
```

| Component | requests cpu | requests memory | limits cpu | limits memory |
|-----------|-------------|----------------|-----------|--------------|
| Coordinator | 250m | 512Mi | 1 | 1Gi |
| Overlord | 250m | 512Mi | 1 | 1Gi |
| Broker | 250m | 512Mi | 1 | 1Gi ⚠️ |
| Historical | 250m | 768Mi | 1 | 1536Mi |
| **Indexer** | **300m** | **1Gi** | **2** | **2Gi ⚠️** |
| Router | 100m | 256Mi | 500m | 512Mi ⚠️ |

### Overriding base ConfigMaps from an overlay

All settings in `base/*-configmap.yaml` can be overridden via overlay patches.

#### How it works

Kustomize's **Strategic Merge Patch** replaces values at the ConfigMap `data` key level.  
Since `runtime.properties` is a single multi-line string key, the **entire key is replaced with the new value**.  
It is not possible to change just one line within a key, so you must include the entire content of the key in the patch file.

```
base/broker-configmap.yaml          overlay patch
─────────────────────────           ──────────────────────────────
data:                               data:
  runtime.properties: |     →         runtime.properties: |
    druid.processing.numThreads=1       druid.processing.numThreads=4  ← changed
    druid.processing.buffer...          druid.processing.buffer...     ← all others included too
    ...                                 ...
```

#### Method 1: Strategic Merge Patch (recommended)

Create a patch file under `overlays/apache-druid-37.0.0/patches/`.

**Example — changing Broker processing thread count**

```yaml
# overlays/apache-druid-37.0.0/patches/broker-tuning.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: druid-broker-config
data:
  runtime.properties: |
    druid.service=druid/broker
    druid.plaintextPort=8082
    druid.broker.http.numConnections=5
    druid.server.http.numThreads=40
    druid.processing.buffer.sizeBytes=100000000   # 50MB → 100MB
    druid.processing.numThreads=4                 # 1 → 4
    druid.processing.numMergeBuffers=2
```

Register in `kustomization.yaml`:

```yaml
patches:
  - path: patches/replicas.yaml
  - path: patches/jvm-heaps.yaml
  - path: patches/k8s-resources.yaml
  - path: patches/broker-tuning.yaml    # ← add this
```

> **Important**: When replacing a `runtime.properties` value, you must include **all original properties**.  
> Any omitted property will be absent from the final ConfigMap.

#### Method 2: JSON 6902 Patch (replace a single key)

Useful when you want to replace only `runtime.properties` or `jvm.config`, not both.

```yaml
# overlays/apache-druid-37.0.0/patches/indexer-jvm.yaml
- op: replace
  path: /data/jvm.config
  value: |-
    -server
    -Duser.timezone=UTC
    -Dfile.encoding=UTF-8
    -XX:+ExitOnOutOfMemoryError
    -XX:+UseG1GC
    -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
    -Djava.io.tmpdir=var/tmp
    -Xms1g
    -Xmx4g                           # 2g → 4g
    -XX:MaxDirectMemorySize=1g       # 512m → 1g
```

Register in `kustomization.yaml` (target must be specified):

```yaml
patches:
  - path: patches/indexer-jvm.yaml
    target:
      kind: ConfigMap
      name: druid-indexer-config
```

#### Patchable keys reference

| ConfigMap | Patchable `data` keys | Common reasons to patch |
|-----------|-----------------------|------------------------|
| `druid-common-config` | `common.runtime.properties` | Storage type, ZK settings, emitter |
| `druid-{component}-config` | `runtime.properties` | Processing buffers, thread count, port |
| `druid-{component}-config` | `jvm.config` | heap, MaxDirect — already handled by `jvm-heaps.yaml` |
| `druid-{component}-config` | `log4j2.xml` | Log level, add appenders |

#### Commonly changed settings and linked changes

| Setting changed | What else must change together |
|-----------------|-------------------------------|
| `processing.numThreads` ↑ | `MaxDirectMemorySize` ↑, `limits memory` ↑ |
| `processing.buffer.sizeBytes` ↑ | `MaxDirectMemorySize` ↑, `limits memory` ↑ |
| `Xmx` ↑ | `limits memory` ↑ (must be ≥ `Xmx + MaxDirect + 300Mi`) |
| `worker.capacity` ↑ | `Xmx` ↑, `limits memory` ↑ |
| `server.http.numThreads` ↑ | Check `limits cpu` |

---

## 5. Storage Component Selection

`base/` and `components/` are kept as read-only reference templates.  
All environment-specific values live in overlay patch files — never edit component files directly.

### PVC Selection (3-tier)

| Configuration | Result |
|--------------|--------|
| `components/pvc` absent | `emptyDir` — local disk, data lost on pod restart |
| `components/pvc` present | PVC with cluster default StorageClass |
| `components/pvc` + `patches/pvc-patch.yaml` | PVC with custom StorageClass / sizes |

### Deep Storage Backend Selection

Enable exactly one patch in `overlays/apache-druid-37.0.0/kustomization.yaml`:

```yaml
patches:
  # Enable exactly one below ↓
  - path: patches/druid-common-config-with-minio.yaml       # ✅ currently active
  # - path: patches/druid-common-config-with-aws-s3.yaml
  # - path: patches/druid-common-config-with-azure.yaml
  # - path: patches/druid-common-config-with-gcs.yaml
  # - path: patches/druid-common-config-with-hdfs.yaml
  # - path: patches/druid-common-config-with-aliyun-oss.yaml
  # - path: patches/druid-common-config-with-cloudfiles.yaml
  # - path: patches/druid-common-config-with-cassandra.yaml

  # Enable when using S3 or MinIO ↓
  - path: patches/statefulsets-with-aws-region.yaml
    target:
      kind: StatefulSet
```

### Required Settings per Storage Backend

Fill in the `<...>` placeholders directly in each `overlays/.../patches/druid-common-config-with-*.yaml`.

**MinIO** (`patches/druid-common-config-with-minio.yaml`)
```properties
druid.s3.accessKey=<MINIO_ACCESSKEY>
druid.s3.secretKey=<MINIO_SECRETKEY>
druid.s3.endpoint.url=http://<MINIO-ENDPOINT>
druid.s3.enablePathStyleAccess=true
```

**AWS S3** (`patches/druid-common-config-with-aws-s3.yaml`)
```properties
druid.s3.accessKey=<AWS_ACCESSKEY>
druid.s3.secretKey=<AWS_SECRETKEY>
druid.storage.bucket=<APACHE-DRUID-BUCKET>
# Also set AWS_REGION in patches/statefulsets-with-aws-region.yaml
```

**Azure Blob** (`patches/druid-common-config-with-azure.yaml`)
```properties
druid.azure.account=<AZURE_STORAGE_ACCOUNT>
druid.azure.key=<AZURE_STORAGE_KEY>
druid.azure.container=druid
```

**GCS** (`patches/druid-common-config-with-gcs.yaml`)
```properties
druid.google.bucket=<GCS_BUCKET>
# No key needed when using Workload Identity
# Explicit key: druid.google.jsonCredentials=<JSON_KEY_PATH>
```

**HDFS** (`patches/druid-common-config-with-hdfs.yaml`)
```properties
druid.storage.storageDirectory=hdfs://<NAMENODE_HOST>:9000/druid/segments
```
> ⚠️ `hdfs-site.xml` and `core-site.xml` must be mounted as a separate ConfigMap.

**Aliyun OSS** (`patches/druid-common-config-with-aliyun-oss.yaml`)
```properties
druid.oss.accessKey=<OSS_ACCESS_KEY>
druid.oss.secretKey=<OSS_SECRET_KEY>
druid.oss.endpoint=<OSS_ENDPOINT>         # e.g. oss-cn-hangzhou.aliyuncs.com
druid.storage.oss.bucket=<OSS_BUCKET>
```

**Cassandra** (`patches/druid-common-config-with-cassandra.yaml`)
> ⚠️ Community extension with limited official documentation. You must create the Cassandra keyspace schema manually before use.

### Storage Backend Summary

| Backend | Extension | `storage.type` | Task Logs | Official Support |
|---------|-----------|---------------|-----------|-----------------|
| S3 / MinIO | `druid-s3-extensions` | `s3` | S3 | ✅ |
| Azure Blob | `druid-azure-extensions` | `azure` | Azure | ✅ |
| GCS | `druid-google-extensions` | `google` | GCS | ✅ |
| HDFS | `druid-hdfs-storage` | `hdfs` | HDFS | ✅ |
| Aliyun OSS | `aliyun-oss-extensions` | `oss` | OSS | Community |
| Rackspace Cloudfiles | `druid-cloudfiles-extensions` | `cloudfiles` | file (fallback) | Community |
| Cassandra | `druid-cassandra-storage` | `c*` | file (fallback) | Community ⚠️ |

---

## 6. Key Design Decisions

### 6-1. MiddleManager → Indexer Migration

**Before**: `MiddleManager` + `Peon` (fork model)  
**Now**: `Indexer` (JVM thread model)

#### Why we migrated

MiddleManager forks a separate **Peon process** for each Task.  
This approach has structural problems in Kubernetes environments:

- Kubernetes isolates resources at the Pod level, but Peons are subprocesses inside a Pod → resource model mismatch
- When a Pod restarts, all running Peons (Tasks) are forcibly terminated
- Forking subprocesses is inherently unstable in container environments

**Indexer** runs Tasks as JVM threads (`ThreadingTaskRunner`):

```
MiddleManager: Pod → [JVM] → fork → [Peon JVM] × N  (separate processes)
Indexer:       Pod → [JVM] ─────── [Thread] × N      (same JVM)
```

- No subprocess → container-friendly
- `worker.capacity=3`: a single Indexer Pod can run up to 3 Tasks concurrently
- JVM heap must accommodate all Tasks combined, so set it generously (`-Xmx2g`)

---

### 6-2. Per-Task Log Files (log4j2 RoutingAppender)

**Problem**: Druid UI Tasks → Logs button returns an empty page  
**Cause**: Indexer uses threads, so no separate log file is created automatically  
**Solution**: Route Task thread logs to files using a log4j2 `RoutingAppender`

#### How it works

Analysis of the Druid source (`Appenderators.setTaskThreadContextForIndexers()`) shows that when a Task thread starts, two MDC keys are automatically set:

```
task.log.id   = taskId
task.log.file = /opt/druid/var/druid/task/slot{N}/{taskId}/log
```

These are used for routing via RoutingAppender:

```xml
<Routing name="TaskRouting">
  <Routes pattern="${{ctx:task.log.id}}">
    <!-- task.log.id is empty (regular server thread) → discard -->
    <Route key="">
      <Null name="Null"/>
    </Route>
    <!-- task.log.id is set (Task thread) → write to file -->
    <Route>
      <File fileName="${{ctx:task.log.file}}" createOnDemand="true"/>
    </Route>
  </Routes>
</Routing>
```

After the Task completes, the Overlord uploads the file to `indexing-logs/` in S3 (MinIO),  
and the Druid UI displays it via `GET /druid/indexer/v1/task/{taskId}/log`.

---

### 6-3. Indexer HA Setup (replicas=2)

The base Indexer defaults to `replicas: 1`. For environments requiring HA, increase it to 2 via an overlay patch.

#### Why 2 replicas

`taskCount=1, replicas=2` is an **HA (high availability) configuration, not throughput scaling**.

```
Kafka topic partitions: [0] [1] [2]

taskCount=1 → 1 Task consumes all partitions
replicas=2  → the same Task runs simultaneously on 2 pods (both consume the same partitions)

Task-A (druid-indexers-0): consumes partitions [0][1][2]
Task-B (druid-indexers-1): consumes partitions [0][1][2]  ← standby role
```

- If druid-indexers-0 dies, Task-B immediately takes over from the last committed offset with no interruption
- Druid manages Kafka offsets, so no data loss or duplication
- To increase throughput, increase `taskCount` instead (up to the number of partitions)

#### How to configure

**1. Change Indexer Pod replicas** — `overlays/apache-druid-37.0.0/patches/replicas.yaml`

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-indexers
spec:
  replicas: 2   # 1(default) → 2(HA)
```

**2. Supervisor settings** — edit the Supervisor spec via Druid UI or API

```json
{
  "ioConfig": {
    "taskCount": 1,
    "replicas": 2
  }
}
```

**3. Check worker.capacity** — `base/indexer-configmap.yaml`

```properties
druid.worker.capacity=3   # concurrent tasks per Pod. With replicas=2, max 6 tasks cluster-wide
```

#### Resource impact

With Indexer replicas=2, each pod requires memory request 1Gi for scheduling.  
The increase from 1Gi → 1Gi × 2 = 2Gi additional capacity was needed,  
which was achieved by first reducing Kafka memory (see 6-4 below).

---

### 6-4. Kafka Resource Optimization

**Problem**: Kafka's default JVM heap of 1GB caused memory shortage  
**Solution**: Lower JVM heap to 256MB and fix requests=limits

```yaml
env:
  - name: KAFKA_HEAP_OPTS
    value: "-Xmx256m -Xms256m"   # default 1GB → 256MB

resources:
  requests:
    cpu: 200m
    memory: 512Mi
  limits:
    cpu: 200m
    memory: 512Mi
```

Measured actual Kafka pod memory usage with heap 256m: ~78Mi  
(KRaft mode, 3 replicas, small topic)

- **requests = limits**: prevents memory over-commit (Burstable → Guaranteed QoS)
- Freed memory provides placement headroom for the second Indexer pod

---

### 6-5. ZooKeeper-free Kubernetes Service Discovery

```properties
druid.zk.service.enabled=false
druid.discovery.type=k8s
druid.discovery.k8s.clusterIdentifier=druid-default
druid.serverview.type=http
druid.coordinator.loadqueuepeon.type=http
druid.indexer.runner.type=httpRemote
```

Since Druid 26+, you can use the Kubernetes API directly for service discovery without ZooKeeper.  
`clusterIdentifier` is used to distinguish multiple Druid clusters deployed in the same namespace.

> **Note**: There is a history of intermittent issues where the Kubernetes watch connection expires every ~60 minutes, causing the Overlord to mistakenly consider the Indexer offline. The following settings now ensure automatic reconnection:
> ```properties
> druid.k8s.apiClientConfig.watchReconnectLimit=-1
> druid.k8s.apiClientConfig.watchReconnectInterval=1000
> ```

---

### 6-6. Deep Storage: S3 (MinIO Compatible)

```properties
druid.storage.type=s3
druid.s3.endpoint.url=http://192.168.0.100:9000
druid.s3.enablePathStyleAccess=true      # required for MinIO
druid.s3.forceGlobalBucketAccessEnabled=false
```

MinIO is used as an S3-compatible storage. Differences from AWS S3:

| Item | AWS S3 | MinIO |
|------|--------|-------|
| `endpoint.url` | Not needed (uses default) | Must be specified |
| `enablePathStyleAccess` | `false` | **`true` required** |
| Authentication | IAM Role / keys | Access keys |

Required MinIO bucket: `druid` (shared for segments + indexing-logs)

---

### 6-7. Kustomize Structure Design Principles

#### base = minimal configuration with no dependencies

`base/` is a **fully functional configuration that can be applied immediately** without MinIO, PVC, or external storage.  
Data is lost on pod restart (emptyDir), but all features work correctly.  
Ideal for development environments or feature testing.

#### Components = reusable feature blocks

Kustomize's `Component` (kind: Component), unlike overlays,  
is a **module that can be shared across multiple overlays**.

```
# Example: when adding a Druid 38.0.0 overlay
overlays/apache-druid-38.0.0/kustomization.yaml
  resources: [../../base]
  components:
    - ../../components/pvc          ← structural change only (emptyDir → PVC)
  patches:
    - patches/pvc-patch.yaml        ← StorageClass / sizes (if needed)
    - patches/druid-common-config-with-minio.yaml  ← storage backend credentials
    - only 38.0.0-specific changes
```

#### Why PVC and storage are separated

```
pvc component           → Kubernetes layer: emptyDir → volumeClaimTemplates
druid-common-config-*   → Druid layer: application storage settings
```

The two concerns are independent, so they are kept separate.  
Example: local PVC + HDFS, emptyDir + S3 — both combinations are possible.

#### Why storage credentials live in overlay patches, not components

`base/` and `components/` are committed to git as read-only reference templates.  
Credentials and environment-specific endpoints go in overlay patch files (`druid-common-config-with-*.yaml`), which can be gitignored or managed separately per environment.

---

## 7. Known Issues

### 7-1. Task "Could not allocate segment" Failure

**Symptom**: Task fails with `Could not allocate segment` error in task logs  
**Cause**: Task runs immediately after cluster restart before segment metadata has fully loaded from the DB  
**Impact**: Already-committed segments are **not deleted** (Druid segments are immutable)  
**Resolution**: Wait for the Coordinator to fully initialize (typically 30–60 seconds) after restart, then restart the Supervisor

### 7-2. Kubernetes Watch Expiry Causes Indexer Offline Detection (resolved)

**Symptom**: Every ~60 minutes, the Overlord considers the Indexer offline and fails to reassign Tasks  
**Cause**: Druid bug where Kubernetes watch connection expires and reconnection fails  
**Resolution**: Fixed with `watchReconnectLimit=-1` (unlimited reconnect) + `watchReconnectInterval=1000ms`

If the issue recurs, an Overlord rollout restart provides immediate recovery.

### 7-3. Completed Task History Lost on Overlord Restart

**Symptom**: After Overlord restart, all previous task entries disappear from the UI Tasks tab  
**Cause**: `druid.indexer.storage.type=local` (default) — Task history is stored only in Overlord memory

```
druid.indexer.storage.type=local    ← stored in Overlord memory only (default)
druid.indexer.storage.type=metadata ← stored persistently in PostgreSQL
```

| Situation | local type | metadata type |
|-----------|-----------|--------------|
| Overlord restart | Task history immediately lost | Preserved |
| Auto-expiry TTL | None | Configurable |
| Storage location | Overlord JVM heap memory | PostgreSQL `druid_tasks` table |

**Base default**: `druid.indexer.storage.type=local` (task history not persisted)

**Resolution**: The overlay's `patches/overlord-configmap-patch.yaml` replaces this with `metadata`.

```
base/overlord-configmap.yaml          overlays/{name}/patches/overlord-configmap-patch.yaml
─────────────────────────────         ──────────────────────────────────────────────
druid.indexer.storage.type=local  →   druid.indexer.storage.type=metadata
```

```yaml
# overlays/{name}/patches/overlord-configmap-patch.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: druid-overlord-config
data:
  runtime.properties: |
    druid.service=druid/overlord
    druid.plaintextPort=8090
    druid.indexer.queue.startDelay=PT30S
    druid.indexer.runner.type=httpRemote
    druid.indexer.storage.type=metadata
    druid.indexer.storage.remote.maxTaskStatusCount=10
    druid.server.http.numThreads=60
```

This is registered in `kustomization.yaml` under `patches:`:

```yaml
patches:
  - path: patches/overlord-configmap-patch.yaml  # Task history: local → metadata (PostgreSQL)
```

#### `druid.indexer.storage.remote.maxTaskStatusCount`

When `storage.type=metadata`, this limits the **maximum number of completed Task records** visible in the Overlord UI / API.

| Item | Details |
|------|---------|
| Default | `10` |
| Effective condition | Only when `storage.type=metadata` |
| Scope | Overlord UI Tasks tab, `/druid/indexer/v1/tasks` API response count |
| DB retention | All history remains in the PostgreSQL `druid_tasks` table regardless of this setting |

The default (10) shows only the 10 most recent entries. To see more history, add it to the overlay patch:

```yaml
    druid.indexer.storage.type=metadata
    druid.indexer.storage.remote.maxTaskStatusCount=100  # add if needed
    druid.server.http.numThreads=60
```

> This setting was removed from base. It operates with the default (10), and is specified only in overlays where needed.

> **Note**: Since the entire `runtime.properties` key is replaced, you must include all existing properties.  
> To change the retention count, simply update the `maxTaskStatusCount` value.

Task log files themselves (uploaded to S3) are preserved regardless of storage.type.  
storage.type only affects the **Task metadata history** visible in the Overlord UI.

### 7-4. Indexer Memory Over-allocation Warning

**Symptom**: `kubectl top node` shows node3 memory at 93% allocated  
**Cause**: Indexer replicas=2, requests=1Gi × 2 = 2Gi both scheduled on node3  
**Impact**: If actual usage stays below requests, OOM does not occur (Kubernetes schedules based on requests)  
**Monitoring**: `kubectl top pod`, `kubectl describe node node3 | grep MemoryPressure`

### 7-5. Druid 37 + Jetty 12 — Router managementProxy Thread Shortage (resolved)

**Symptom**: Router Pod CrashLoopBackOff or error immediately after startup

```
Insufficient configured threads: required=2 < max=2 for AsyncManagementForwardingServlet
```

**Background**

Jetty was upgraded from **Druid 35.0.0 onwards to version 12.x** (34.0.0 was the last version with Jetty 9.x).

| Druid Version | Jetty Version | managementProxy | K8s discovery |
|---------------|--------------|----------------|---------------|
| 34.0.0 | 9.x | Normal | Unstable (stale watch events) |
| 35.0.0+ / 37.0.0 | 12.x | **Bug** (default) | Stable |
| **37.0.0 + workaround** | **12.x** | **Normal** | **Stable** |

**Root Cause**

When Router's `managementProxy` creates an internal HTTP client, the thread pool size is calculated as:

```
max = druid.router.http.numConnections / 25
```

Default `numConnections=50` → `max=2`. Jetty 12 subtracts 1 reserved thread,  
so actual usable threads = 1 → below the 2 needed by the selector → error.

In a single-CPU Pod (container) where `Runtime.availableProcessors() = 1`, Jetty 12  
calculates `thread pool max=2`, compounding the bug.

**Resolution**: Add `-XX:ActiveProcessorCount=4` to the Router JVM

```properties
# overlays/apache-druid-37.0.0/patches/jvm-heaps.yaml → router jvm.config
-server
-XX:ActiveProcessorCount=4   ← added: JVM sees 4 cores → Jetty thread pool max=4+
-Xms256m
-Xmx256m
-XX:MaxDirectMemorySize=128m
```

`availableProcessors()=4` → `max = 4` → after Jetty 12 reserves 1 thread, 3 usable → error resolved.

**Approaches tried but not recommended**

```properties
# Method A: disable managementProxy (Coordinator/Overlord API proxy unavailable in web console)
druid.router.managementProxy.enabled=false

# Method B: increase numConnections directly (the root availableProcessors issue remains)
druid.router.http.numConnections=200   # 200/25 = 8 threads → provides headroom
```

Method A causes the `Load data` setup in the web console to stop working,  
so `-XX:ActiveProcessorCount=4` was adopted as the final solution.

### 7-6. Control-plane (plane1) Memory Growth — apiserver Watch Accumulation

> **Note**: After deploying Druid, plane1 (control-plane) memory grows gradually before stabilizing at a certain level. This is expected behavior.

**Symptom**

When a Druid cluster starts with `druid.discovery.type=k8s`, plane1 memory increases progressively.

**Cause — apiserver watch connection accumulation**

Druid's `druid-kubernetes-extensions` opens a watch connection to kube-apiserver when each component starts, to detect Pod changes in real time. The apiserver maintains this watch state in memory, so plane1 memory grows proportionally to the number of Druid Pods.

```
Druid component starts
  → druid-kubernetes-extensions loads
  → registers Pod watch with kube-apiserver (plane1)
  → apiserver maintains watch state in memory
  → plane1 memory starts increasing
```

**Commands to verify**

```bash
kubectl get --raw /metrics \
  | grep 'apiserver_longrunning_requests' \
  | grep -v '^#' \
  | grep 'WATCH' \
  | grep -v '} 0' \
  | sort -t'}' -k2 -rn \
  | head -25
```

**Actual measurements (Druid 37.0.0 in production)**

```
apiserver_longrunning_requests{resource="configmaps", scope="resource", verb="WATCH"} 41
apiserver_longrunning_requests{resource="services",   scope="cluster", verb="WATCH"} 25
apiserver_longrunning_requests{resource="pods",       scope="namespace",verb="WATCH"} 24  ← Druid main contributor
apiserver_longrunning_requests{resource="endpointslices",scope="cluster",verb="WATCH"} 18
apiserver_longrunning_requests{resource="nodes",      scope="cluster", verb="WATCH"} 17
apiserver_longrunning_requests{resource="namespaces", scope="cluster", verb="WATCH"} 17
apiserver_longrunning_requests{resource="pods",       scope="cluster", verb="WATCH"} 16
apiserver_longrunning_requests{resource="nodes",      scope="resource",verb="WATCH"} 15
apiserver_longrunning_requests{resource="ippools",    scope="cluster", verb="WATCH"} 10  ← Calico
```

**Watch sources**

| Resource | Count | Primary source |
|----------|-------|----------------|
| `pods` namespace-scope | 24 | **Druid K8s discovery** (each component watches pods) |
| `pods` cluster-scope | 16 | kube-controller-manager, Calico |
| `configmaps` resource-scope | 41 | Druid + system components |
| `services`, `endpointslices` | 25, 18 | kube-proxy, Calico, MetalLB |
| `ippools`, `ipamblocks` etc. | 10, 6 | Calico CNI |
| `ipaddresspools`, `communities` etc. | 5 | MetalLB |

The 6 Druid component types (Coordinator, Overlord, Broker, Historical, Indexer×2, Router) each watch Pods in the default namespace, so a significant portion of the 24 `pods namespace-scope` watches is attributable to Druid.

**Memory growth pattern**

```
Right after deploy  → watch connections established sequentially → memory grows gradually
After stabilization → watch count fixed → memory held at steady level
Druid restart       → watch reconnections → temporary spike, then stabilizes again
```

**How to determine if this is abnormal**

- Memory grows and then **stops → normal**
- Memory **keeps growing without stopping** → watch connections are continuously reconnecting — check `watchReconnectLimit=-1` setting

```bash
# Verify Druid correctly recognizes Indexer (proof that watch is alive)
kubectl exec druid-overlords-0 -- wget -qO- localhost:8090/druid/indexer/v1/workers \
  | python3 -m json.tool
```

If the Indexer appears in the worker list, the watch connection is working normally.

---

## 8. Troubleshooting Guide

### 8-1. Basic Diagnostic Commands

#### Pod Status

```bash
# All pod status (including node placement and IP)
kubectl get pod -o wide

# Filter pods that are not Running
kubectl get pod | grep -v Running | grep -v Completed

# Pod details — Events, restart reasons, mount errors, etc.
kubectl describe pod <pod-name>

# Previous container logs (when CrashLoopBackOff)
kubectl logs <pod-name> --previous

# Live log stream
kubectl logs <pod-name> -f --tail=100
```

#### Events

```bash
# Recent Warning events (OOMKilled, Evicted, FailedScheduling, etc.)
kubectl get events --field-selector type=Warning --sort-by='.lastTimestamp'

# Events for a specific pod only
kubectl get events --field-selector involvedObject.name=<pod-name>
```

#### Resource Usage

```bash
# Actual CPU/memory usage per pod
kubectl top pod --sort-by=memory

# Per-node usage
kubectl top node

# Node requests/limits allocation (check for 93% warnings, etc.)
kubectl describe node node1 node2 node3 | grep -A6 "Allocated resources"

# MemoryPressure status
kubectl describe node node1 node2 node3 | grep MemoryPressure
```

---

### 8-2. Per-component Log Inspection

```bash
# Query all logs at once by label
kubectl logs -l nodeType=coordinator --tail=100
kubectl logs -l nodeType=overlord    --tail=100
kubectl logs -l nodeType=broker      --tail=100
kubectl logs -l nodeType=historical  --tail=100
kubectl logs -l nodeType=indexer     --tail=200   # includes task logs
kubectl logs -l nodeType=router      --tail=100

# When Indexer replicas=2, check each pod separately
kubectl logs druid-indexers-0 --tail=200
kubectl logs druid-indexers-1 --tail=200
```

#### Direct Task Log Inspection (inside Indexer pod)

```bash
# List task log files (running or completed)
kubectl exec druid-indexers-0 -- find /opt/druid/var/druid/task -name "log" -type f

# Print a specific task log
kubectl exec druid-indexers-0 -- tail -200 /opt/druid/var/druid/task/slot0/<taskId>/log
```

---

### 8-3. Checking Internal State via Druid API

After port-forward, use curl to directly query each component's internal state.

```bash
# Coordinator — segment load status
kubectl port-forward svc/druid-coordinators 8081:8081 &
curl -s http://localhost:8081/status/health
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus | python3 -m json.tool

# Overlord — task list and status
kubectl port-forward svc/druid-overlords 8090:8090 &
curl -s http://localhost:8090/druid/indexer/v1/tasks | python3 -m json.tool
curl -s "http://localhost:8090/druid/indexer/v1/task/<taskId>/status"

# Overlord — registered workers (Indexers)
curl -s http://localhost:8090/druid/indexer/v1/workers | python3 -m json.tool

# Indexer — worker enabled status
kubectl port-forward svc/druid-indexers 8091:8091 &
curl -s http://localhost:8091/druid/worker/v1/enabled

# Historical — loaded segment list
kubectl port-forward svc/druid-historicals 8083:8083 &
curl -s http://localhost:8083/druid/historical/v1/loadedSegments | python3 -m json.tool

# Router (UI API) — cluster health
kubectl port-forward svc/druid-routers 8888:8888 &
curl -s http://localhost:8888/status/health
curl -s http://localhost:8888/druid/v2/datasources
```

---

### 8-4. Actual Errors Encountered and Resolutions

#### Error 1: Tasks → Logs button shows blank page

**How to check**
```bash
# Verify that log files are created in the task directory inside the Indexer pod
kubectl exec druid-indexers-0 -- ls -la /opt/druid/var/druid/task/
```

**Cause**  
Indexer uses threads, so no separate log file is created automatically.  
Druid UI requests the file via `GET /druid/indexer/v1/task/{taskId}/log`; if the file doesn't exist, the response is empty.

**Resolution**: Add RoutingAppender to `log4j2.xml` in `base/indexer-configmap.yaml`

```xml
<Routing name="TaskRouting">
  <Routes pattern="${{ctx:task.log.id}}">
    <Route key=""><Null name="Null"/></Route>
    <Route>
      <File fileName="${{ctx:task.log.file}}" createOnDemand="true"/>
    </Route>
  </Routes>
</Routing>
```

`Appenderators.setTaskThreadContextForIndexers()` in Druid source automatically sets  
MDC keys `task.log.id` and `task.log.file` when a Task thread starts — use these as the routing key.

**Remaining WARN log after applying RoutingAppender**

Even after applying RoutingAppender, the following WARN appears in Indexer logs. **It is harmless.**

```
WARN [qtp755477196-91] org.apache.druid.indexing.worker.http.WorkerResource - Failed to read log for task: index_kafka_druid_6e4a4defbfb2f30_idnpaofk
java.io.FileNotFoundException: /opt/druid/var/druid/task/slot2/index_kafka_druid_.../log (No such file or directory)
```

**Cause**: When the Overlord or UI pre-fetches the log of a task that is still starting or just beginning, the file doesn't exist yet because `createOnDemand="true"` only creates the file when the first log line is written. Once the Task actually starts writing logs, the file is created and subsequent fetches succeed.

**Action required**: None. There is no functional issue and UI log viewing works correctly during task execution.

---

#### Error 2: Overlord recognizes MiddleManager (Worker) as offline every ~60 minutes

**How to check**
```bash
# Search Overlord logs for worker offline messages
kubectl logs -l nodeType=overlord --tail=500 | grep -i "worker\|offline\|peon\|fork"

# Check current worker registrations via Overlord API
kubectl port-forward svc/druid-overlords 8090:8090 &
curl -s http://localhost:8090/druid/indexer/v1/workers
```

**Root cause**: Missing Kubernetes Discovery label patch on MiddleManager startup

When Druid operates with `druid.discovery.type=k8s`, the MiddleManager Pod patches the  
`druidDiscoveryAnnouncement-cluster-identifier=druid-default` label onto itself at startup.  
Without this label, the Overlord cannot recognize the Pod as a Worker.

```
Normal startup flow:
  MiddleManager starts → patches label on its Pod via Kubernetes API → Overlord watch detects → Worker registered

Problem flow:
  MiddleManager starts → label patch fails or timing missed
                     → Overlord cannot detect in watch → sees worker=0
                     → Tasks cannot be assigned
```

**Immediate recovery (when using MiddleManager)**

```bash
# Restart MiddleManager → label re-patched → Overlord rediscovers worker
kubectl rollout restart statefulset/druid-middlemanagers

# Verify worker registration
curl -s http://localhost:8090/druid/indexer/v1/workers | python3 -m json.tool
```

**Permanent fix**: Replace MiddleManager with Indexer

Indexer discovers itself through the Kubernetes Service (`druid-indexers`) instead of  
the `druidDiscoveryAnnouncement` label patch approach.  
Since the label patch step is eliminated, this issue is structurally resolved.

```
MiddleManager: Overlord → [K8s watch] → Pod label detected → Worker registered  (label patch needed)
Indexer:       Overlord → [K8s Service] → druid-indexers → Worker registered (no label patch)
```

The following watch reconnect settings in `common-configmap.yaml` also prevent intermittent connection expiry:
```properties
druid.k8s.apiClientConfig.watchReconnectLimit=-1
druid.k8s.apiClientConfig.watchReconnectInterval=1000
```

---

#### Error 3: Task FAILED — "Could not allocate segment"

**How to check**
```bash
# Find root cause in task logs
kubectl exec druid-indexers-0 -- grep -r "Could not allocate" /opt/druid/var/druid/task/

# Check if Coordinator has finished initializing
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus
```

**Cause**  
Task runs right after cluster restart before the Coordinator has finished reading segment information from the metadata DB.

**Resolution**  
Wait 30–60 seconds after restart, then restart the Supervisor:

```bash
# Suspend and resume the Supervisor
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisorId>/suspend
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisorId>/resume
```

> Already-committed segments are not deleted. Just re-run the failed Task.

---

#### Error 4: MinIO (S3) Connection Failure — "Access Denied" or connection refused

**How to check**
```bash
# Search Indexer logs for S3 errors
kubectl logs -l nodeType=indexer --tail=300 | grep -i "s3\|access\|endpoint"

# Test MinIO connectivity (inside Indexer pod)
kubectl exec druid-indexers-0 -- curl -v http://<MINIO_HOST>:9000/druid
```

**Causes and resolutions**

| Symptom | Cause | Resolution |
|---------|-------|------------|
| `Access Denied` | `enablePathStyleAccess=false` | Set `druid.s3.enablePathStyleAccess=true` |
| `Connection refused` | endpoint URL not set | Specify `druid.s3.endpoint.url=http://<MINIO_HOST>:9000` |
| Bucket not found | `druid` bucket not created in MinIO | Create the bucket in the MinIO console |

```properties
# Required settings in components/storage/s3/patch.yaml
druid.s3.endpoint.url=http://<MINIO_HOST>:9000
druid.s3.enablePathStyleAccess=true
druid.s3.forceGlobalBucketAccessEnabled=false
```

---

#### Error 5: Kubernetes API Watch Expiry — Overlord intermittently marks Indexer offline

> This is a separate issue that can occur even after migrating from MiddleManager to Indexer.

**How to check**
```bash
# Search Overlord logs for watch expiry messages
kubectl logs -l nodeType=overlord --tail=500 | grep -i "watch\|reconnect\|offline"
```

**Cause**  
The watch connection Druid holds to the Kubernetes API Server is terminated by the server after ~60 minutes.  
If Druid fails to reconnect, the Overlord cannot update the Indexer state and marks it offline.

**Resolution**: Add to `base/common-configmap.yaml`

```properties
druid.k8s.apiClientConfig.watchReconnectLimit=-1       # unlimited reconnect attempts
druid.k8s.apiClientConfig.watchReconnectInterval=1000  # reconnect every 1 second
```

---

#### Error 6: OOMKilled or Node Memory Overload

**How to check**
```bash
# Find OOMKilled pods
kubectl get pod -o json | python3 -c "
import json, sys
pods = json.load(sys.stdin)
for p in pods['items']:
    for cs in p.get('status', {}).get('containerStatuses', []):
        last = cs.get('lastState', {}).get('terminated', {})
        if last.get('reason') == 'OOMKilled':
            print(p['metadata']['name'], cs['name'], last)
"

# Per-node allocation status
kubectl describe node | grep -E "Name:|memory|cpu" | grep -A2 "Allocated"

# Real-time usage
kubectl top pod --sort-by=memory
```

**Cause**: Kafka default JVM heap 1GB + Indexer replicas=2 (1Gi×2) = insufficient node memory

**Resolution**: Reduce Kafka heap to 256MB to free space for the second Indexer Pod

```yaml
# kafka StatefulSet env
- name: KAFKA_HEAP_OPTS
  value: "-Xmx256m -Xms256m"   # default 1GB → 256MB
```

Measured actual Kafka Pod usage: ~78Mi with heap 256m (KRaft, 3 replicas).

---

### 8-5. Quick Diagnostic Script

Prints the entire cluster status at once.

```bash
#!/bin/bash
echo "=== Pod Status ==="
kubectl get pod -o wide

echo ""
echo "=== Pods Not Ready ==="
kubectl get pod | grep -Ev "Running|Completed|NAME"

echo ""
echo "=== Recent Warning Events ==="
kubectl get events --field-selector type=Warning --sort-by='.lastTimestamp' | tail -15

echo ""
echo "=== Resource Usage ==="
kubectl top pod --sort-by=memory 2>/dev/null || echo "(metrics-server not installed)"

echo ""
echo "=== Node Memory Pressure ==="
kubectl describe node $(kubectl get node -o name | sed 's|node/||') \
  | grep -E "Name:|MemoryPressure"
```

---

## 9. Kafka Configuration Reference

KRaft mode (no ZooKeeper required), 3 replicas. Uses a Headless Service for Pod-to-Pod communication.

### Headless Service (`02-kafka-svc.yaml`)

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
    - name: internal
      port: 9092
    - name: controller
      port: 9093
```

### StatefulSet (`05-kafka-stateful-set.yaml`)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      securityContext:
        fsGroup: 1001
      volumes:
        - name: kafka-config
          emptyDir: {}
      initContainers:
        - name: init-node-id
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              ORDINAL=$(hostname | awk -F'-' '{print $NF}')
              echo "$ORDINAL" > /config/node-id
          volumeMounts:
            - name: kafka-config
              mountPath: /config
        - name: format-storage
          image: apache/kafka:4.2.0
          command:
            - sh
            - -c
            - |
              NODE_ID=$(cat /config/node-id)
              if [ ! -f "/data/meta.properties" ]; then
                echo "node.id=$NODE_ID" > /tmp/kraft.properties
                echo "process.roles=broker,controller" >> /tmp/kraft.properties
                echo "controller.quorum.voters=0@kafka-0.kafka-headless.default.svc.cluster.local:9093,1@kafka-1.kafka-headless.default.svc.cluster.local:9093,2@kafka-2.kafka-headless.default.svc.cluster.local:9093" >> /tmp/kraft.properties
                echo "listeners=PLAINTEXT://:9092,CONTROLLER://:9093" >> /tmp/kraft.properties
                echo "advertised.listeners=PLAINTEXT://localhost:9092" >> /tmp/kraft.properties
                echo "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" >> /tmp/kraft.properties
                echo "inter.broker.listener.name=PLAINTEXT" >> /tmp/kraft.properties
                echo "controller.listener.names=CONTROLLER" >> /tmp/kraft.properties
                echo "log.dirs=/data" >> /tmp/kraft.properties
                /opt/kafka/bin/kafka-storage.sh format \
                  --ignore-formatted \
                  --cluster-id q1Sh-9_ISia_zwGINzRvyQ \
                  --config /tmp/kraft.properties
              fi
          volumeMounts:
            - name: kafka-data
              mountPath: /data
            - name: kafka-config
              mountPath: /config
      containers:
        - name: kafka
          image: apache/kafka:4.2.0
          command:
            - sh
            - -c
            - |
              export KAFKA_NODE_ID=$(cat /config/node-id)
              exec /etc/kafka/docker/run
          ports:
            - containerPort: 9092
            - containerPort: 9093
          env:
            - name: CLUSTER_ID
              value: "q1Sh-9_ISia_zwGINzRvyQ"
            - name: KAFKA_PROCESS_ROLES
              value: "broker,controller"
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: "CONTROLLER"
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: "0@kafka-0.kafka-headless.default.svc.cluster.local:9093,1@kafka-1.kafka-headless.default.svc.cluster.local:9093,2@kafka-2.kafka-headless.default.svc.cluster.local:9093"
            - name: KAFKA_LISTENERS
              value: "INTERNAL://:9092,CONTROLLER://:9093"
            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
              value: "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
            - name: KAFKA_INTER_BROKER_LISTENER_NAME
              value: "INTERNAL"
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "INTERNAL://$(POD_NAME).kafka-headless.default.svc.cluster.local:9092"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "2"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "2"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "2"
            - name: KAFKA_MIN_INSYNC_REPLICAS
              value: "2"
            - name: KAFKA_LOG_DIRS
              value: /data
            - name: KAFKA_HEAP_OPTS
              value: "-Xmx256m -Xms256m"   # default 1GB → 256MB (to free memory for Indexer replicas=2)
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: 200m
              memory: 512Mi
          volumeMounts:
            - name: kafka-data
              mountPath: /data
            - name: kafka-config
              mountPath: /config
  volumeClaimTemplates:
    - metadata:
        name: kafka-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

### Cluster ID Generation and Replacement

`--cluster-id` is a **URL-safe Base64-encoded UUID** (22 characters) that identifies the KRaft cluster.  
All Pods' initContainers and env must share the same ID.

#### When to replace the Cluster ID

| Situation | Action |
|-----------|--------|
| New deployment | Generate a new ID and update the yaml |
| Fully reinitializing (discarding all data) | New ID + delete PVCs |
| Maintaining existing cluster (redeploy, config change) | **Keep the same ID** (changing it makes existing data unrecognizable) |

#### How to generate a new Cluster ID

```bash
# Method 1: Use kafka-storage.sh via Docker
docker run --rm apache/kafka:4.2.0 /opt/kafka/bin/kafka-storage.sh random-uuid

# Method 2: Python (same format as kafka-storage.sh)
python3 -c "import uuid, base64; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode())"

# Method 3: From an already running kafka Pod
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-storage.sh random-uuid
```

Replace the generated ID in **both of these places**:

```yaml
# initContainer format-storage
/opt/kafka/bin/kafka-storage.sh format \
  --cluster-id <NEW_CLUSTER_ID> \    ← here
  ...

# container env
- name: CLUSTER_ID
  value: "<NEW_CLUSTER_ID>"           ← here
```

> ⚠️ Changing the ID makes existing PVC data unusable. Delete and recreate the PVCs as well:
> ```bash
> kubectl delete statefulset kafka
> kubectl delete pvc -l app=kafka
> kubectl apply -f 02-kafka-svc.yaml -f 05-kafka-stateful-set.yaml
> ```

### Supervisor Configuration (Connecting Druid to Kafka)

```json
{
  "type": "kafka",
  "spec": {
    "dataSchema": { ... },
    "ioConfig": {
      "type": "kafka",
      "consumerProperties": {
        "bootstrap.servers": "kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092,kafka-2.kafka-headless:9092"
      },
      "taskCount": 1,
      "replicas": 2,
      "taskDuration": "PT1H"
    }
  }
}
```

---

## 10. Druid Architecture — Component Roles and Data Flow

### 10-1. Component Roles

```
┌─────────────────────────────────────────────────────────────────┐
│                        Query / Ingest                           │
│                                                                 │
│   Client ──→ Router ──→ Broker ──→ Historical                   │
│                    └──→ Overlord ──→ Indexer (real-time segs)   │
│                    └──→ Coordinator                             │
│                                                                 │
│                  [PostgreSQL: metadata]                         │
│                  [S3/MinIO: Deep Storage]                       │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Port | Role |
|-----------|------|------|
| **Coordinator** | 8081 | Segment distribution management. Instructs Historical nodes on which segments to load/drop. Manages retention policies and auto-compaction |
| **Overlord** | 8090 | Task lifecycle management. Creates/terminates Supervisors, assigns Tasks to Indexers, records task status in the metadata DB |
| **Broker** | 8082 | Query router. Inspects the segment timeline to distribute sub-queries to Historical/Indexer nodes, then merges and returns the result |
| **Historical** | 8083 | Immutable segment serving. Downloads segments from Deep Storage, caches them locally, and responds to queries |
| **Indexer** | 8091 | Ingest task execution (JVM thread model). Real-time indexing of new data and segment upload to Deep Storage |
| **Router** | 8888 | API gateway + web console. Proxies queries to Broker, Task API to Overlord, segment API to Coordinator |

#### Component Data Stores

| Store | Role | In this deployment |
|-------|------|-------------------|
| **Deep Storage** | Permanent storage for segment files (.smoosh) | S3/MinIO (`druid` bucket) |
| **Metadata Store** | Segment registry, task history, Supervisor state | PostgreSQL (`druid-postgresql:5432`) |
| **Local Cache** | Segments downloaded by Historical for serving | PVC (`/opt/druid/var`) |

---

### 10-2. Kafka → Druid Ingestion Flow

```
Kafka Topic
  │
  │  (1) Supervisor assigns partitions
  ▼
Indexer Task (real-time consumption)
  │  · Reads messages via Kafka Consumer
  │  · Adds rows to in-memory Incremental Index
  │  · On taskDuration (default 1h) or row count threshold → Publish
  │
  │  (2) Publish phase
  ├──▶ Uploads segment file to Deep Storage (S3)
  ├──▶ Registers segment in PostgreSQL metadata DB
  └──▶ New Task picks up from the next Kafka offset
         │
         │  (3) Coordinator detects
         ▼
     Historical Node
       · Downloads segment from S3 → local cache
       · Subsequent queries answered by Historical
```

#### Supervisor State Machine

```
PENDING ──▶ RUNNING ──▶ SUSPENDED
                │             │
                ▼             ▼
            STOPPING      RUNNING (on resume)
                │
                ▼
           UNHEALTHY (on consecutive Task failures)
```

```bash
# List Supervisors and their status
curl -s http://localhost:8090/druid/indexer/v1/supervisor | python3 -m json.tool

# Detailed status of a specific Supervisor
curl -s http://localhost:8090/druid/indexer/v1/supervisor/<id>/status

# Kafka offset consumption (per-partition lag)
curl -s http://localhost:8090/druid/indexer/v1/supervisor/<id>/stats
```

#### Supervisor Parameters: taskCount vs replicas

The two parameters serve **different purposes**.

| Parameter | Role | Effect |
|-----------|------|--------|
| `taskCount` | Split partitions for parallel processing | **Throughput ↑** |
| `replicas` | Duplicate processing of the same data | **High availability (HA) ↑** |

**Behavior per configuration**

| Config | Task count | Partition distribution | Effect |
|--------|-----------|----------------------|--------|
| `taskCount=1, replicas=2` (current) | 2 | Each Task consumes all of P0,P1,P2 | HA (fault tolerance) |
| `taskCount=2, replicas=1` | 2 | Task-A → P0,P1 / Task-B → P2 | 2× throughput |
| `taskCount=2, replicas=2` | 4 | Above config with a replica for each | HA + 2× throughput |

**Current configuration (taskCount=1, replicas=2) — HA behavior**

```
Kafka Topic (partition 0, 1, 2)
           │
     ┌─────┴─────┐
     │  Overlord  │  ← Supervisor creates and manages 2 Tasks
     └─────┬─────┘
     ┌─────┴──────────────────┐
     ▼                        ▼
  Task-A                   Task-B
(consumes P0, P1, P2)   (consumes P0, P1, P2)
     │                        │
druid-indexers-0         druid-indexers-1

Task-A fails
  → Task-B immediately resumes from last committed offset (no data loss)
  → Supervisor reassigns a new Task-A to druid-indexers-0
  → Both Tasks resume simultaneous consumption (HA restored)
```

**Throughput scaling (taskCount=2, replicas=1)**

```
Kafka Topic (partition 0, 1, 2)
           │
     ┌─────┴─────┐
     │  Overlord  │  ← Supervisor creates 2 Tasks
     └─────┬─────┘
     ┌─────┴──────────────────┐
     ▼                        ▼
  Task-A                   Task-B
(handles P0, P1)          (handles P2)
     │                        │
druid-indexers-0         druid-indexers-1
```

> `taskCount` cannot exceed the number of Kafka partitions. With 3 partitions, `taskCount` max is 3.

```json
// Example: throughput + HA simultaneously (3 partitions, 6 Tasks)
"taskCount": 3,
"replicas": 2
```

#### Supervisor Task States: active vs publishing

On the Supervisor screen, you may see both states simultaneously:

```
(1 task × 2 replicas)
2 active tasks
2 publishing tasks
```

These two states represent **different generations of tasks**.

| State | What it does | Kafka reads | Deep storage upload |
|-------|-------------|-------------|---------------------|
| **active** | Actively consuming Kafka partitions | ✅ continuously reading | ❌ |
| **publishing** | Finalizing/pushing segments | ❌ reading stopped | ✅ in progress |

**Task lifecycle:**

```
[active]  reading data from Kafka
    ↓  segment granularity boundary reached or task.duration expired
[publishing]  Kafka reading stops → segments merged → pushed to deep storage → metadata registered
    ↓  complete
[done]  Historical loads segment → queryable
```

When an active task reaches a segment boundary:
1. The task transitions to **publishing** state (starts pushing to S3/MinIO)
2. Supervisor **immediately creates a new active task** — no gap in Kafka reads
3. When the publishing task completes, Historical loads the segment

So `2 active + 2 publishing` is **normal**. The previous generation is completing its push while the new generation is already reading from Kafka.

---

### 10-3. Query Execution Flow

```
Client (SQL / Native JSON)
  │
  ▼
Router :8888
  │  · queries → proxied to Broker
  │  · Task API → proxied to Overlord
  │
  ▼
Broker :8082
  │  (1) Query segment timeline
  │      · caches "which Historical holds which segments" from Coordinator
  │      · caches "real-time segment range" from Indexer
  │
  │  (2) Distribute sub-queries
  ├──▶ Historical :8083  (published segments)
  └──▶ Indexer :8091    (real-time segments not yet published)
         │
         │  (3) Each node returns partial results from its segments
         ▼
Broker (4) Merges partial results → applies final aggregation/sort/LIMIT
  │
  ▼
Client ← final response
```

#### Segment Selection Criteria for Queries

```
Query: SELECT ... WHERE __time BETWEEN '2025-01-01' AND '2025-01-02'

What Broker checks:
  1. List of segments covering that time range (from metadata cache)
  2. Which Historical/Indexer holds each segment
  3. If duplicate segments exist, only the highest-priority version is selected (compaction results take precedence)
```

#### Query Performance Factors

| Factor | Impact | Tuning point |
|--------|--------|-------------|
| Historical count | Parallel segment processing | replicas, add nodes |
| Broker heap | Memory for merging | increase `-Xmx` |
| Segment size | File I/O | merge via compaction |
| Segment cache | Avoids re-downloading from S3 | increase Historical PVC size |
| Broker query cache | Accelerates repeated queries | `druid.broker.cache.*` |

```bash
# Check segment count and size currently loaded
curl -s http://localhost:8081/druid/coordinator/v1/datasources/<datasource>/segments \
  | python3 -c "import json,sys; segs=json.load(sys.stdin); print(f'Total {len(segs)} segments')"

# Check Broker's segment timeline cache
curl -s http://localhost:8082/druid/broker/v1/loadstatus

# Check query execution plan (SQL)
curl -s -X POST http://localhost:8082/druid/v2/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "EXPLAIN PLAN FOR SELECT COUNT(*) FROM <datasource>"}' \
  | python3 -m json.tool
```

---

### 10-4. Coordinator — Segment Lifecycle

```
Indexer uploads segment
  │
  ▼
Registered in PostgreSQL metadata DB (used=1)
  │
  ▼
Coordinator detects (polling period: druid.coordinator.period=PT30S)
  │
  ├──▶ Check retention policy
  │      · Segments past retention period → used=0 (logical delete)
  │      · Physical delete from Deep Storage is performed by a separate Kill Task
  │
  ├──▶ Check replication rules
  │      · replicants=1 → load on 1 Historical only
  │      · replicants=2 → replicate to 2 Historicals
  │
  └──▶ Instruct load
         Historical: "download this segment from S3 and start serving"
```

```bash
# Segment load progress
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus

# Unloaded segments (not yet received by Historical)
curl -s http://localhost:8081/druid/coordinator/v1/loadqueue

# Delete a segment (logical delete)
curl -X DELETE "http://localhost:8081/druid/coordinator/v1/datasources/<datasource>/intervals/<interval>"
```

---

### 10-5. MiddleManager vs Indexer — Ingestion Flow Comparison

#### MiddleManager approach (previous configuration)

```
[Router UI] Load data setup
    ↓
[Router] managementProxy → forwards to Overlord API
    ↓
[Overlord] Registers Supervisor and runs continuously
    ↓
[Overlord] Decides to create Task every taskDuration → requests Task assignment to MiddleManager
    ↓
[MiddleManager] Receives Task → forks Peon process (runs separate JVM)
    ↓
[Peon JVM] Kafka consuming → in-memory buffering → segment creation → S3 upload
    ↓
[Overlord] Supervisor detects taskDuration expiry
    ├── Sends stop signal to Peon
    └── Assigns new Task to MiddleManager → forks new Peon
    ↓
[Peon] S3 upload complete → writes segment metadata to PostgreSQL
    ↓
[Coordinator] Detects new segment via PostgreSQL polling (30-second interval)
    ↓
[Historical] Downloads segment from S3 → local PVC cache → ready to serve queries
```

#### Indexer approach (current configuration)

```
[Router UI] Load data setup
    ↓
[Router] managementProxy → forwards to Overlord API
    ↓
[Overlord] Registers Supervisor and runs continuously
    ↓
[Overlord] Decides to create Task every taskDuration → requests Task assignment to Indexer
    ↓
[Indexer JVM] Executes Task as a JVM thread (no fork)
              └── MDC keys set → log4j2 RoutingAppender → writes logs to file
    ↓
[Indexer Thread] Kafka consuming → in-memory buffering → segment creation → S3 upload
    ↓
[Overlord] Supervisor detects taskDuration expiry
    ├── Sends stop signal to thread (interrupt)
    └── Starts new Task thread (no new fork needed)
    ↓
[Indexer] S3 upload complete → writes segment metadata to PostgreSQL
    ↓
[Coordinator] Detects new segment via PostgreSQL polling (30-second interval)
    ↓
[Historical] Downloads segment from S3 → local PVC cache → ready to serve queries
```

#### Comparison

| Item | MiddleManager | Indexer (current) |
|------|--------------|-------------------|
| Task execution | Separate Peon process (fork) | Thread within the same JVM |
| Log files | Auto-created per process → uploaded to S3 | Requires RoutingAppender setup |
| UI log viewing | Works by default | Works after applying RoutingAppender |
| K8s resource model | Mismatch with Pod-level isolation | Pod = JVM = Task group, aligned |
| Worker registration | Pod label patch (`druidDiscoveryAnnouncement-*`) | Kubernetes Service-based discovery |
| Tasks on failure | Pod restart = all Peons forcibly killed | Threads stop, JVM continues |
| `worker.capacity` | Max concurrent Peons per MiddleManager | Max concurrent Task threads per Indexer |

#### worker.capacity Setting

```properties
# base/indexer-configmap.yaml → runtime.properties
druid.worker.capacity=3   # maximum concurrent tasks a single Indexer Pod can handle
```

`replicas=2` (2 Indexer Pods) + `worker.capacity=3` = maximum 6 Tasks cluster-wide.  
Increasing `worker.capacity` means the JVM must accommodate all Tasks together, so also increase `-Xmx`.

---

## Reference Links

- [Apache Druid 37.0.0 Documentation](https://druid.apache.org/docs/37.0.0/)
- [Druid Kubernetes Extensions Configuration](https://druid.apache.org/docs/latest/development/extensions-core/kubernetes)
- [Druid Task Logging](https://druid.apache.org/docs/latest/configuration/#task-logging)
- [Druid Extensions List](https://druid.apache.org/docs/latest/configuration/extensions)
- [Kustomize Components](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/components/)
