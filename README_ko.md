🇺🇸 [English](README.md) | 🇰🇷 한국어

# Apache Druid 37.0.0 — Kubernetes Kustomize 배포 가이드

> Operator 없이 StatefulSet으로 직접 배포하는 Apache Druid 37.0.0 Kubernetes 구성.  
> 스토리지 백엔드를 컴포넌트 단위로 교체할 수 있도록 Kustomize Components 패턴으로 구성되어 있습니다.

---

## 목차

1. [인프라 구성](#1-인프라-구성)
2. [전체 Kustomize 구조](#2-전체-kustomize-구조)
3. [사용법 (Quick Start)](#3-사용법-quick-start)
4. [각 파일 설명](#4-각-파일-설명)
5. [스토리지 컴포넌트 선택](#5-스토리지-컴포넌트-선택)
6. [주요 설계 결정 및 이유](#6-주요-설계-결정-및-이유)
7. [알려진 이슈](#7-알려진-이슈) (7-5: Jetty 12 Router 스레드 부족 포함)
8. [트러블슈팅 가이드](#8-트러블슈팅-가이드)
9. [Kafka 구성 참고](#9-kafka-구성-참고)
10. [Druid 아키텍처 — 컴포넌트 역할 및 데이터 흐름](#10-druid-아키텍처--컴포넌트-역할-및-데이터-흐름)

---

## 1. 인프라 구성

### Druid 컴포넌트 구성

| 컴포넌트 | 포트 | replicas (기본) | 역할 |
|----------|------|----------------|------|
| Coordinator | 8081 | 1 | 세그먼트 배치 및 로드 관리 |
| Overlord | 8090 | 1 | Task 스케줄링 및 Worker 관리 |
| Broker | 8082 | 1 | 쿼리 라우팅 및 결과 병합 |
| Historical | 8083 | 1 | 세그먼트 로드 및 쿼리 처리 |
| Indexer | 8091 | 1 | Kafka ingest + Task 실행 (JVM thread 방식) |
| Router | 8888 | 1 | UI / API Gateway |

> Indexer를 2개로 늘리면 HA 구성이 됩니다. 방법과 이유는 [6-3. Indexer HA 구성](#6-3-indexer-ha-구성-replicas2)을 참고하세요.

---

## 2. 전체 Kustomize 구조

```
apache-druid-kustomize/
│
├── base/                              # 기본 구성 (local storage + emptyDir)
│   ├── kustomization.yaml             # 리소스 목록
│   ├── rbac.yaml                      # ServiceAccount, Role, RoleBinding
│   ├── postgres-secret.yaml           # PostgreSQL Secret (기본값)
│   ├── postgres.yaml                  # PostgreSQL Service + StatefulSet
│   ├── common-configmap.yaml          # Druid 공통 설정 (local storage)
│   ├── {component}-configmap.yaml     # 각 컴포넌트 JVM / log4j2 / runtime.properties
│   └── {component}.yaml              # StatefulSet + Service (emptyDir 사용)
│
├── components/                        # 재사용 가능한 기능 모듈
│   ├── pvc/                           # emptyDir → PVC 교체
│   │   └── kustomization.yaml         # JSON 6902 패치 (7개 StatefulSet 전체)
│   └── storage/                       # 스토리지 백엔드 선택
│       ├── s3/                        # S3 / MinIO (AWS S3 호환)
│       ├── azure/                     # Microsoft Azure Blob Storage
│       ├── gcs/                       # Google Cloud Storage
│       ├── hdfs/                      # Apache HDFS
│       ├── aliyun-oss/                # Alibaba Cloud OSS (커뮤니티)
│       ├── cloudfiles/                # Rackspace Cloud Files (커뮤니티)
│       └── cassandra/                 # Apache Cassandra (커뮤니티, 주의)
│
└── overlays/
    ├── apache-druid-37.0.0/           # 참조 overlay — git에 커밋됨
    │   ├── kustomization.yaml         # 컴포넌트 선택 + 패치 참조
    │   ├── postgres.env               # PostgreSQL 자격증명 (.gitignored)
    │   ├── postgres.env.example       # 자격증명 템플릿 (git 관리)
    │   └── patches/
    │       ├── replicas.yaml                          # Indexer replicas = 2
    │       ├── jvm-heaps.yaml                         # 각 컴포넌트 JVM heap 크기
    │       ├── k8s-resources.yaml                     # K8s requests / limits
    │       ├── overlord-configmap-patch.yaml          # Task 이력: local → metadata (PostgreSQL)
    │       ├── pvc-patch.yaml                         # PVC 크기 (StorageClass 재정의)
    │       ├── druid-common-config-with-minio.yaml    # ← 현재 활성 스토리지
    │       ├── druid-common-config-with-aws-s3.yaml   #   (하나만 활성화)
    │       ├── druid-common-config-with-azure.yaml
    │       ├── druid-common-config-with-gcs.yaml
    │       ├── druid-common-config-with-hdfs.yaml
    │       ├── druid-common-config-with-aliyun-oss.yaml
    │       ├── druid-common-config-with-cloudfiles.yaml
    │       ├── druid-common-config-with-cassandra.yaml
    │       └── statefulsets-with-aws-region.yaml      # S3/MinIO: AWS_REGION 주입
    └── <your-cluster>/                # 사용자 overlay — gitignored
        └── (apache-druid-37.0.0 복사 후 필요한 부분만 수정)
```

### 설계 원칙

```
base/        emptyDir + local storage 최소 구성.
             수정하지 않음 — 읽기 전용 참조로 취급.

components/  선택적 재사용 모듈 (PVC, 스토리지 백엔드).
             수정하지 않음 — overlay에서 참조해서 사용.

overlays/    환경별 최종 구성.
             참조 overlay를 복사하고 달라지는 부분만 수정.
```

**Git 정책**: `base/`와 `components/`는 그대로 커밋. `overlays/apache-druid-37.0.0/`는 참조용으로 커밋. 다른 overlay 디렉토리는 모두 gitignore — 환경별로 git 외부에서 관리하거나 시크릿 도구로 관리.

---

## 3. 사용법 (Quick Start)

### 나만의 Overlay 만들기

`overlays/apache-druid-37.0.0/`는 git에 커밋된 참조 overlay입니다.  
새 환경을 만들 때는 이 디렉토리를 복사하고 달라지는 부분만 수정합니다 — base/나 components/는 건드리지 않습니다.

```bash
# 참조 overlay 복사
cp -rp overlays/apache-druid-37.0.0 overlays/<your-cluster>

# postgres.env 생성
cp overlays/<your-cluster>/postgres.env.example \
   overlays/<your-cluster>/postgres.env
vi overlays/<your-cluster>/postgres.env

# kustomization.yaml 수정: 스토리지 백엔드, PVC 여부 등
vi overlays/<your-cluster>/kustomization.yaml

# 활성 스토리지 패치에 자격증명 입력
vi overlays/<your-cluster>/patches/druid-common-config-with-minio.yaml
```

### 배포

```bash
# 내 overlay 배포
kubectl apply -k overlays/<your-cluster>/

# base만 배포 (local storage + emptyDir, dev/test용)
kubectl apply -k base/

# dry-run으로 확인
kubectl kustomize overlays/<your-cluster>/ | kubectl apply --dry-run=client -f -
```

### 배포 확인

```bash
kubectl get pod -o wide
kubectl get pvc
kubectl get svc
```

### Druid UI 접근

Router에 외부 접근하려면 `components/router-external` 컴포넌트를 활성화합니다.  
이 컴포넌트는 `type: LoadBalancer` 서비스(`druid-router-external`)를 생성합니다.

```yaml
# overlays/apache-druid-37.0.0/kustomization.yaml
components:
  - ../../components/router-external   # 활성화 시 LoadBalancer 서비스 생성
```

적용 후 External IP 확인:

```bash
kubectl get svc druid-router-external
# NAME                    TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)
# druid-router-external   LoadBalancer   10.96.x.x      <YOUR-LB-IP>     80:xxxxx/TCP
```

LoadBalancer IP가 할당되면 브라우저에서 `http://<EXTERNAL-IP>` 로 접근합니다.

External IP 없이 로컬에서 접근할 경우 port-forward 사용:

```bash
kubectl port-forward svc/druid-routers 8888:8888
# http://localhost:8888 으로 접근
```

### Druid 버전 업그레이드

overlay 디렉토리명과 `kustomization.yaml`의 `images.newTag`가 버전을 함께 관리합니다.  
base의 StatefulSet은 `image: apache/druid` (태그 없음)로 선언되어 있고, overlay가 태그를 주입합니다.

```bash
# 새 버전 overlay 생성 (예: 38.0.0)
cp -r overlays/apache-druid-37.0.0 overlays/apache-druid-38.0.0

# overlays/apache-druid-38.0.0/kustomization.yaml 에서 한 줄만 변경
images:
  - name: apache/druid
    newTag: "38.0.0"   ← 여기만 수정

# 배포
kubectl apply -k overlays/apache-druid-38.0.0/
```

### 업데이트 적용

```bash
# 설정 변경 후
kubectl apply -k overlays/apache-druid-37.0.0/

# 특정 컴포넌트만 재시작
kubectl rollout restart statefulset/druid-indexers
```

---

## 4. 각 파일 설명

### `base/common-configmap.yaml`

Druid 전체 컴포넌트가 공유하는 공통 설정 (`common.runtime.properties`).

| 설정 키 | 기본값 | 설명 |
|---------|--------|------|
| `druid.discovery.type` | `k8s` | ZooKeeper 없이 Kubernetes API로 서비스 발견 |
| `druid.discovery.k8s.clusterIdentifier` | `druid-default` | 클러스터 구분자 (네임스페이스별로 고유하게) |
| `druid.storage.type` | `local` | base 기본값; overlay로 s3/azure/gcs 등 교체 |
| `druid.metadata.storage.type` | `postgresql` | 메타데이터 DB 종류 |
| `druid.emitter` | `prometheus` | 메트릭 수집 방식 |

### `base/{component}-configmap.yaml`

각 컴포넌트별 ConfigMap. 3개의 파일 키를 가집니다:

```
runtime.properties  → 컴포넌트별 포트, 역할, 처리 설정
jvm.config          → JVM 힙, GC, 다이렉트 메모리 설정
log4j2.xml          → 로그 출력 설정 (Indexer는 RoutingAppender 포함)
```

### `base/indexer-configmap.yaml` — 특수 설정

Indexer의 `log4j2.xml`에는 **RoutingAppender**가 포함되어 있습니다.  
Task 실행 중 MDC 키 `task.log.id`가 설정되면 자동으로 파일에도 기록됩니다.

```xml
<Routing name="TaskRouting">
  <Routes pattern="${{ctx:task.log.id}}">
    <Route key="">          <!-- task 아닐 때 → /dev/null -->
      <Null name="Null"/>
    </Route>
    <Route>                 <!-- task 실행 중 → 파일 기록 -->
      <File fileName="${{ctx:task.log.file}}" .../>
    </Route>
  </Routes>
</Routing>
```

이를 통해 Druid UI의 **Tasks → Logs** 버튼이 정상 동작합니다.

### `base/{component}.yaml`

StatefulSet + Service 정의. base에서는 모두 `emptyDir` 사용:

```yaml
volumes:
  - name: common-config
    configMap:
      name: druid-common-config
  - name: node-config
    configMap:
      name: druid-{component}-config
  - name: data
    emptyDir: {}            # overlay의 pvc 컴포넌트가 PVC로 교체
```

#### Pod 환경변수 — POD_NAME / POD_NAMESPACE / POD_IP

모든 Druid StatefulSet에 아래 env가 공통으로 들어있습니다:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name        # 예: druid-brokers-0
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace   # 예: default
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP         # 예: 10.233.102.5
  - name: druid_host
    value: "$(POD_IP)"                  # Druid가 자신의 주소로 사용
```

**각 변수의 역할과 넣은 이유:**

| 변수 | 위치 | 용도 | 없을 때 문제 |
|------|------|------|-------------|
| `POD_NAME` | base | `druid-kubernetes-extensions`가 자신의 Pod 라벨 패치 시 Pod 이름으로 사용 | 라벨 패치 실패 → Overlord가 worker 미인식 |
| `POD_NAMESPACE` | base | Kubernetes API 호출 시 namespace 특정 | cross-namespace 오동작 |
| `POD_IP` | base | `druid_host` 값으로 주입 — Druid가 클러스터 내 자신의 주소를 알리는 데 사용 | hostname으로 fallback → 통신 실패 |
| `druid_host` | base | `druid.host` 설정을 env로 override | — |
| `AWS_REGION` | **overlay patch만** | AWS SDK가 S3 엔드포인트 리전 결정에 사용 | S3/MinIO 연결 실패 가능 |

**`druid_host=$(POD_IP)`가 필요한 이유:**

Druid 37.0.0에서 `druid.host`를 명시하지 않으면 JVM이 `InetAddress.getLocalHost().getHostName()`으로 hostname을 반환합니다. Kubernetes Pod의 hostname(`druid-brokers-0`)은 클러스터 내 다른 Pod에서 DNS 없이 직접 통신할 수 없습니다. Pod IP를 직접 사용해야 컴포넌트 간 HTTP 통신이 정상 동작합니다.

```
hostname 사용 시: druid-brokers-0 → DNS 조회 실패 or 잘못된 IP → 통신 불가
POD_IP 사용 시:  10.233.102.5    → 직접 통신 가능 ✅
```

> 이 문제는 Druid 37.0.0 업그레이드 중 전체 Pod 간 통신 실패로 발견됐습니다.

**`AWS_REGION`이 base에 없는 이유:**

S3/MinIO를 사용하지 않는 환경(local emptyDir, Azure, GCS 등)에는 `AWS_REGION`이 불필요합니다.  
S3/MinIO를 사용할 때는 overlay patch를 활성화하면 모든 StatefulSet에 자동으로 주입됩니다:

```yaml
# overlays/<your-cluster>/patches/statefulsets-with-aws-region.yaml
- op: add
  path: /spec/template/spec/containers/0/env/-
  value:
    name: AWS_REGION
    value: "us-east-1"   # MinIO는 값 무관, AWS S3는 버킷 리전으로 변경
```

MinIO 사용 시 `AWS_REGION` 값은 무관합니다(`druid.s3.endpoint.url`로 엔드포인트를 직접 지정하기 때문).  
실제 AWS S3 사용 시에는 버킷이 있는 리전(예: `ap-northeast-2`)으로 변경해야 합니다.

### `components/pvc/kustomization.yaml`

JSON 6902 패치로 `emptyDir`을 제거하고 `volumeClaimTemplates`를 추가합니다.  
`storageClassName`은 지정하지 않음 — 클러스터 기본 StorageClass가 자동 적용됩니다.

```
PostgreSQL  → 8Gi  (메타데이터 DB, 반드시 PVC 권장)
Coordinator → 5Gi
Overlord    → 5Gi
Broker      → 10Gi
Historical  → 10Gi (세그먼트 캐시)
Indexer     → 10Gi (task 임시 디렉토리)
Router      → 5Gi
```

StorageClass나 크기를 바꾸려면 component를 수정하지 않고 overlay patch를 사용합니다:

```yaml
# overlays/<your-cluster>/patches/pvc-patch.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-historicals
spec:
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        storageClassName: longhorn   # 클러스터 기본값 재정의
        resources:
          requests:
            storage: 50Gi           # 크기 재정의
```

### `overlays/<your-cluster>/patches/replicas.yaml`

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-indexers
spec:
  replicas: 2    # HA 구성: 각 pod가 모든 Kafka 파티션을 독립 소비
```

### `overlays/apache-druid-37.0.0/patches/jvm-heaps.yaml`

각 컴포넌트의 `jvm.config` 키를 교체합니다.

| 컴포넌트 | `-Xmx` | `-XX:MaxDirectMemorySize` | 비고 |
|----------|--------|--------------------------|------|
| Coordinator | 256m | 256m | 메타 관리 위주, 충분 |
| Overlord | 256m | 256m | Task 스케줄링 위주 |
| Broker | 512m | 256m | 복잡한 쿼리 시 증가 고려 |
| Historical | 512m | 256m | 세그먼트 수 비례 |
| Indexer | **2g** | 512m | Task 동시 실행 수(worker.capacity)에 비례 |
| Router | 256m | 128m | UI + 라우팅 위주 |

#### `-Djava.io.tmpdir=var/tmp` — 볼륨과의 연관 관계

모든 컴포넌트 `jvm.config`에 공통으로 들어있는 이 옵션은 볼륨 마운트 구조와 직접 연결됩니다.

```
Druid 프로세스 워킹 디렉토리: /opt/druid/
-Djava.io.tmpdir=var/tmp (상대 경로)
              └→ 실제 경로: /opt/druid/var/tmp
```

볼륨 마운트 구조와 대응:

```
volume: data  (emptyDir 또는 PVC)
  mountPath: /opt/druid/var              ← 볼륨 루트
             /opt/druid/var/tmp          ← tmpdir  ★ 여기
             /opt/druid/var/druid/segments    ← 세그먼트 캐시 (Historical)
             /opt/druid/var/druid/task/       ← Task 임시 파일 (Indexer)
             /opt/druid/var/druid/indexing-logs/ ← Task 로그 파일
```

tmpdir를 볼륨 안으로 지정하는 이유:

| 문제 | 설명 |
|------|------|
| 컨테이너 root fs 용량 제한 | 기본 `/tmp`는 컨테이너 ephemeral storage 할당량 소비 → 대용량 Task 실행 시 디스크 부족 |
| Task 임시 파일 크기 | Indexer는 세그먼트 생성 중 수백 MB~수 GB의 임시 파일 생성 |
| PVC와 일관성 | `components/pvc` 적용 시 PVC가 `/opt/druid/var`를 덮으므로 tmpdir도 자동으로 PVC 공간 사용 |

> base(emptyDir) → PVC로 교체할 때 이 옵션은 수정 불필요.  
> `mountPath: /opt/druid/var`가 동일하게 유지되므로 tmpdir 경로가 자동으로 PVC를 가리킵니다.

#### `processing.*` 설정과 `-XX:MaxDirectMemorySize` 연동

`druid.processing.*` 설정은 **Direct Memory(off-heap)를 직접 소비**합니다.  
`MaxDirectMemorySize`를 이 값보다 작게 설정하면 `OutOfMemoryError: Direct buffer memory`가 발생합니다.

```
Direct 실제 사용량 = (numThreads + numMergeBuffers) × sizeBytes + Netty IO 오버헤드(~50~80MB)
```

| 컴포넌트 | `sizeBytes` | `numThreads` | `numMergeBuffers` | Processing Direct | Netty IO | 합계 | `MaxDirectMemorySize` | 여유 |
|----------|------------|-------------|------------------|------------------|---------|------|----------------------|------|
| Broker | 50MB | 1 | 2 | (1+2)×50 = 150MB | ~50MB | ~200MB | 256m | ~56MB ✅ |
| Historical | 50MB | 1 | 2 | (1+2)×50 = 150MB | ~50MB | ~200MB | 256m | ~56MB ✅ |
| Indexer | 100MB | 2 | 2 | (2+2)×100 = 400MB | ~80MB | ~480MB | 512m | ~32MB ⚠️ |

> **Indexer 주의**: 여유가 ~32MB밖에 없습니다.  
> `numThreads` 또는 `sizeBytes` 증가 시 반드시 `MaxDirectMemorySize`를 함께 올려야 합니다.  
> `worker.capacity=3`으로 task가 동시에 실행되면 task당 추가 버퍼가 소비될 수 있습니다.

**설정 변경 시 재계산 예시** (numThreads 2→4로 늘릴 경우):
```
(4+2) × 100MB = 600MB + ~80MB ≈ 680MB
→ MaxDirectMemorySize를 512m → 768m 이상으로 올릴 것
→ k8s-resources.yaml의 limits memory도 함께 재계산 필요
```

### `overlays/apache-druid-37.0.0/patches/k8s-resources.yaml`

컨테이너 메모리 `limits`는 **JVM 전체 메모리 사용량**을 수용해야 합니다.  
`limits`가 부족하면 Linux OOM Killer가 프로세스를 강제 종료합니다 (`OOMKilled`).

> `-XX:+ExitOnOutOfMemoryError`는 JVM heap OOM만 처리합니다.  
> 컨테이너 레벨 OOM Killer는 이 옵션으로 막을 수 없습니다.

**메모리 limit 계산 공식**:
```
limits >= Xmx + MaxDirectMemorySize + JVM overhead
                                      (metaspace, code cache,
                                       thread stack, GC 오버헤드)
                                       ≈ 200~350Mi
```

**컴포넌트별 계산 및 현재 설정 상태**:

| 컴포넌트 | Xmx | MaxDirect | overhead | 필요량 | limit | 상태 |
|----------|-----|-----------|---------|--------|-------|------|
| Coordinator | 256m | 256m | ~300Mi | ~812Mi | 1Gi | ✅ 여유 212Mi |
| Overlord | 256m | 256m | ~300Mi | ~812Mi | 1Gi | ✅ 여유 212Mi |
| Broker | 512m | 256m | ~300Mi | ~1068Mi | 1Gi | ⚠️ 수식 한계 (-44Mi) |
| Historical | 512m | 256m | ~300Mi | ~1068Mi | 1536Mi | ✅ 여유 468Mi |
| **Indexer** | **2048m** | **512m** | ~300Mi | **~2860Mi** | **2Gi** | ⚠️ **수식 초과 (-812Mi)** |
| Router | 256m | 128m | ~300Mi | ~684Mi | 512Mi | ⚠️ 수식 초과 (-172Mi) |

> **Broker / Router**: 쿼리량 증가 또는 부하 테스트 시 OOMKilled 발생 가능.  
> Broker → `1536Mi`, Router → `768Mi` 이상으로 올리는 것을 권장합니다.
>
> **Indexer**: `Xmx(2048Mi) + MaxDirect(512Mi) = 2560Mi`로 limit(2Gi=2048Mi)를 이미 초과.  
> heap과 direct memory가 동시에 최대치에 도달하지 않아 현재 운영 가능하지만,  
> `worker.capacity` 증가 또는 대용량 세그먼트 처리 시 OOMKilled 위험이 있습니다.  
> 안전하게 운영하려면 limit을 `3Gi` 이상으로 올리세요 (replicas=2이면 노드당 6Gi 확보 필요).

**OOMKilled 확인 방법**:
```bash
# 재시작된 Pod의 종료 원인 확인
kubectl describe pod <pod-name> | grep -A5 "Last State"

# 전체 Pod 중 OOMKilled 이력 검색
kubectl get pod -o json | python3 -c "
import json, sys
for p in json.load(sys.stdin)['items']:
  for c in p.get('status',{}).get('containerStatuses',[]):
    t = c.get('lastState',{}).get('terminated',{})
    if t.get('reason') == 'OOMKilled':
      print(p['metadata']['name'], c['name'], t.get('finishedAt',''))"
```

| 컴포넌트 | requests cpu | requests memory | limits cpu | limits memory |
|----------|-------------|----------------|-----------|--------------|
| Coordinator | 250m | 512Mi | 1 | 1Gi |
| Overlord | 250m | 512Mi | 1 | 1Gi |
| Broker | 250m | 512Mi | 1 | 1Gi ⚠️ |
| Historical | 250m | 768Mi | 1 | 1536Mi |
| **Indexer** | **300m** | **1Gi** | **2** | **2Gi ⚠️** |
| Router | 100m | 256Mi | 500m | 512Mi ⚠️ |

### overlay에서 base ConfigMap 재설정하기

`base/*-configmap.yaml`의 설정은 overlay 패치로 모두 재정의할 수 있습니다.

#### 동작 원리

Kustomize의 **Strategic Merge Patch**는 ConfigMap의 `data` 키 단위로 값을 교체합니다.  
`runtime.properties`는 여러 줄 문자열 하나의 키이므로, **키 전체를 새 값으로 대체**하는 방식입니다.  
파일 안의 특정 프로퍼티 한 줄만 변경하는 것은 불가능하므로, 변경하려는 키의 내용 전체를 패치 파일에 작성해야 합니다.

```
base/broker-configmap.yaml          overlay patch
─────────────────────────           ──────────────────────────────
data:                               data:
  runtime.properties: |     →         runtime.properties: |
    druid.processing.numThreads=1       druid.processing.numThreads=4  ← 변경
    druid.processing.buffer...          druid.processing.buffer...     ← 나머지도 전부 포함
    ...                                 ...
```

#### 방법 1: Strategic Merge Patch (권장)

패치 파일을 `overlays/apache-druid-37.0.0/patches/` 아래에 생성합니다.

**예시 — Broker processing 스레드 수 변경**

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

`kustomization.yaml`에 등록:

```yaml
patches:
  - path: patches/replicas.yaml
  - path: patches/jvm-heaps.yaml
  - path: patches/k8s-resources.yaml
  - path: patches/broker-tuning.yaml    # ← 추가
```

> **주의**: `runtime.properties` 값을 교체할 때 원본의 **모든 프로퍼티를 포함**해야 합니다.  
> 누락된 프로퍼티는 최종 ConfigMap에서 사라집니다.

#### 방법 2: JSON 6902 Patch (특정 키만 교체)

`runtime.properties`와 `jvm.config` 중 하나만 교체할 때 유용합니다.

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

`kustomization.yaml`에 등록 (target 명시 필요):

```yaml
patches:
  - path: patches/indexer-jvm.yaml
    target:
      kind: ConfigMap
      name: druid-indexer-config
```

#### 패치 대상 키 일람

| ConfigMap | 패치 가능한 `data` 키 | 주요 변경 이유 |
|-----------|----------------------|---------------|
| `druid-common-config` | `common.runtime.properties` | 스토리지 타입, ZK 설정, emitter |
| `druid-{component}-config` | `runtime.properties` | processing 버퍼, 스레드 수, 포트 |
| `druid-{component}-config` | `jvm.config` | heap, MaxDirect — `jvm-heaps.yaml`이 이미 담당 |
| `druid-{component}-config` | `log4j2.xml` | 로그 레벨, appender 추가 |

#### 자주 변경하는 설정과 연동 주의사항

| 변경 설정 | 연동해서 같이 변경해야 할 것 |
|-----------|---------------------------|
| `processing.numThreads` ↑ | `MaxDirectMemorySize` ↑, `limits memory` ↑ |
| `processing.buffer.sizeBytes` ↑ | `MaxDirectMemorySize` ↑, `limits memory` ↑ |
| `Xmx` ↑ | `limits memory` ↑ (`Xmx + MaxDirect + 300Mi` 이상) |
| `worker.capacity` ↑ | `Xmx` ↑, `limits memory` ↑ |
| `server.http.numThreads` ↑ | `limits cpu` 확인 |

---

## 5. 스토리지 컴포넌트 선택

`base/`와 `components/`는 읽기 전용 참조 템플릿으로 유지합니다.  
환경별 설정값은 모두 overlay patch 파일에서 관리합니다 — component 파일을 직접 수정하지 않습니다.

### PVC 선택 (3단계)

| 구성 | 결과 |
|------|------|
| `components/pvc` 없음 | `emptyDir` — 로컬 디스크, Pod 재시작 시 데이터 유실 |
| `components/pvc` 있음 | 클러스터 기본 StorageClass로 PVC 생성 |
| `components/pvc` + `patches/pvc-patch.yaml` | StorageClass / 크기를 overlay에서 재정의 |

### 딥 스토리지 백엔드 선택

`overlays/<your-cluster>/kustomization.yaml`에서 하나만 활성화합니다:

```yaml
patches:
  # 아래 중 하나만 활성화 ↓
  - path: patches/druid-common-config-with-minio.yaml       # ✅ 현재 활성
  # - path: patches/druid-common-config-with-aws-s3.yaml
  # - path: patches/druid-common-config-with-azure.yaml
  # - path: patches/druid-common-config-with-gcs.yaml
  # - path: patches/druid-common-config-with-hdfs.yaml
  # - path: patches/druid-common-config-with-aliyun-oss.yaml
  # - path: patches/druid-common-config-with-cloudfiles.yaml
  # - path: patches/druid-common-config-with-cassandra.yaml

  # S3 / MinIO 사용 시 활성화 ↓
  - path: patches/statefulsets-with-aws-region.yaml
    target:
      kind: StatefulSet
```

### 스토리지별 필수 설정값

각 `overlays/<your-cluster>/patches/druid-common-config-with-*.yaml`에서 `<...>` 플레이스홀더를 교체합니다.

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
# patches/statefulsets-with-aws-region.yaml에서 AWS_REGION도 설정
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
# Workload Identity 사용 시 key 불필요
# 명시적 key: druid.google.jsonCredentials=<JSON_KEY_PATH>
```

**HDFS** (`patches/druid-common-config-with-hdfs.yaml`)
```properties
druid.storage.storageDirectory=hdfs://<NAMENODE_HOST>:9000/druid/segments
```
> ⚠️ `hdfs-site.xml`, `core-site.xml`을 별도 ConfigMap으로 마운트해야 합니다.

**Aliyun OSS** (`patches/druid-common-config-with-aliyun-oss.yaml`)
```properties
druid.oss.accessKey=<OSS_ACCESS_KEY>
druid.oss.secretKey=<OSS_SECRET_KEY>
druid.oss.endpoint=<OSS_ENDPOINT>         # 예: oss-cn-hangzhou.aliyuncs.com
druid.storage.oss.bucket=<OSS_BUCKET>
```

**Cassandra** (`patches/druid-common-config-with-cassandra.yaml`)
> ⚠️ 커뮤니티 확장, 공식 문서 미비. 사용 전 Cassandra keyspace 스키마를 직접 생성해야 합니다.

### 지원 스토리지 요약

| 백엔드 | Extension | `storage.type` | Task 로그 | 공식 지원 |
|--------|-----------|---------------|----------|----------|
| S3 / MinIO | `druid-s3-extensions` | `s3` | S3 | ✅ |
| Azure Blob | `druid-azure-extensions` | `azure` | Azure | ✅ |
| GCS | `druid-google-extensions` | `google` | GCS | ✅ |
| HDFS | `druid-hdfs-storage` | `hdfs` | HDFS | ✅ |
| Aliyun OSS | `aliyun-oss-extensions` | `oss` | OSS | 커뮤니티 |
| Rackspace Cloudfiles | `druid-cloudfiles-extensions` | `cloudfiles` | file (fallback) | 커뮤니티 |
| Cassandra | `druid-cassandra-storage` | `c*` | file (fallback) | 커뮤니티 ⚠️ |

---

## 6. 주요 설계 결정 및 이유

### 6-1. MiddleManager → Indexer 전환

**기존**: `MiddleManager` + `Peon` (fork 방식)  
**현재**: `Indexer` (JVM thread 방식)

#### 왜 전환했는가

MiddleManager는 Task마다 별도의 **Peon 프로세스**를 fork합니다.  
Kubernetes 환경에서는 이 방식에 구조적 문제가 있습니다:

- Kubernetes는 Pod 단위로 리소스를 격리하는데, Peon은 Pod 안의 subprocess → 리소스 모델 불일치
- Pod가 재시작되면 실행 중이던 모든 Peon(Task)이 강제 종료
- subprocess fork 자체가 컨테이너 환경에서 불안정

**Indexer**는 Task를 JVM 스레드(`ThreadingTaskRunner`)로 실행합니다:

```
MiddleManager: Pod → [JVM] → fork → [Peon JVM] × N  (별도 프로세스)
Indexer:       Pod → [JVM] ─────── [Thread] × N      (같은 JVM)
```

- subprocess 없음 → 컨테이너 친화적
- `worker.capacity=3`: 한 Indexer Pod이 최대 3개 Task 동시 실행
- JVM 메모리가 모든 Task를 포함해야 하므로 heap을 충분히 설정 (`-Xmx2g`)

---

### 6-2. Per-Task 로그 파일 (log4j2 RoutingAppender)

**문제**: Druid UI의 Tasks → Logs 버튼이 빈 화면을 반환  
**원인**: Indexer는 Thread 방식이라 별도 log 파일이 자동 생성되지 않음  
**해결**: log4j2 `RoutingAppender`로 Task 스레드 로그를 파일에 라우팅

#### 동작 원리

Druid 소스(`Appenderators.setTaskThreadContextForIndexers()`)를 분석한 결과, Task 스레드가 시작될 때 두 개의 MDC 키가 자동으로 설정됩니다:

```
task.log.id   = taskId
task.log.file = /opt/druid/var/druid/task/slot{N}/{taskId}/log
```

이를 이용해 RoutingAppender로 라우팅합니다:

```xml
<Routing name="TaskRouting">
  <Routes pattern="${{ctx:task.log.id}}">
    <!-- task.log.id가 비어 있으면 (일반 서버 스레드) → 무시 -->
    <Route key="">
      <Null name="Null"/>
    </Route>
    <!-- task.log.id가 있으면 (Task 스레드) → 파일 기록 -->
    <Route>
      <File fileName="${{ctx:task.log.file}}" createOnDemand="true"/>
    </Route>
  </Routes>
</Routing>
```

Task 완료 후 Overlord가 해당 파일을 S3(MinIO)의 `indexing-logs/` 경로로 업로드하고,  
Druid UI는 이를 `GET /druid/indexer/v1/task/{taskId}/log` API로 표시합니다.

---

### 6-3. Indexer HA 구성 (replicas=2)

base의 Indexer는 `replicas: 1`(기본값)입니다. HA가 필요한 환경에서는 overlay patch로 2로 늘립니다.

#### 왜 2개가 필요한가

`taskCount=1, replicas=2`는 **처리량 분산이 아닌 HA(고가용성)** 구성입니다.

```
Kafka 토픽 파티션: [0] [1] [2]

taskCount=1 → Task 1개가 모든 파티션 소비
replicas=2  → 동일한 Task가 2개 동시 실행 (양쪽이 같은 파티션을 소비)

Task-A (druid-indexers-0): 파티션 [0][1][2] 전부 소비
Task-B (druid-indexers-1): 파티션 [0][1][2] 전부 소비  ← 대기 역할
```

- druid-indexers-0이 죽으면 Task-B가 즉시 이어받아 재시작 없이 연속 처리
- Druid가 Kafka offset을 관리하므로 중복/유실 없음
- 처리량을 늘리려면 `taskCount`를 높여야 함 (파티션 수 이하로)

#### 설정 방법

**1. Indexer Pod replicas 변경** — `overlays/apache-druid-37.0.0/patches/replicas.yaml`

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: druid-indexers
spec:
  replicas: 2   # 1(기본) → 2(HA)
```

**2. Supervisor 설정** — Druid UI 또는 API에서 Supervisor spec 수정

```json
{
  "ioConfig": {
    "taskCount": 1,
    "replicas": 2
  }
}
```

**3. worker.capacity 확인** — `base/indexer-configmap.yaml`

```properties
druid.worker.capacity=3   # Pod당 동시 Task 수. replicas=2면 총 최대 6 Task
```

#### 리소스 영향

Indexer replicas=2이므로, 노드 배치 시 각각 memory request 1Gi씩 필요합니다.  
기존 1Gi → 1Gi × 2 = 2Gi 추가 확보가 필요했고,  
이를 위해 Kafka의 메모리를 먼저 절감했습니다 (아래 6-4 참고).

---

### 6-4. Kafka 리소스 최적화

**문제**: Kafka 기본 JVM heap이 1GB로 설정되어 있어 메모리 부족  
**해결**: JVM heap을 256MB로 낮추고 requests=limits로 고정

```yaml
env:
  - name: KAFKA_HEAP_OPTS
    value: "-Xmx256m -Xms256m"   # 기본 1GB → 256MB

resources:
  requests:
    cpu: 200m
    memory: 512Mi
  limits:
    cpu: 200m
    memory: 512Mi
```

실제 측정 결과 heap 256m 설정 후 Kafka pod 실제 메모리 사용량: ~78Mi  
(KRaft 모드, 3 replica, 소규모 토픽 기준)

- **requests = limits**: 메모리 오버커밋 방지 (Burstable → Guaranteed QoS)
- 절감된 메모리로 Indexer 2번째 pod 배치 공간 확보

---

### 6-5. ZooKeeper 없는 Kubernetes 서비스 발견

```properties
druid.zk.service.enabled=false
druid.discovery.type=k8s
druid.discovery.k8s.clusterIdentifier=druid-default
druid.serverview.type=http
druid.coordinator.loadqueuepeon.type=http
druid.indexer.runner.type=httpRemote
```

Druid 26+ 부터 ZooKeeper를 사용하지 않고 Kubernetes API를 직접 이용해 서비스를 발견할 수 있습니다.  
`clusterIdentifier`는 같은 네임스페이스에 여러 Druid 클러스터를 배포할 때 구분하는 식별자입니다.

> **주의**: Kubernetes watch connection이 ~60분마다 만료되면 Indexer가 오프라인으로 인식되는 버그가  
> 간헐적으로 발생한 이력이 있습니다. 현재는 아래 설정으로 자동 재연결을 보장합니다:
> ```properties
> druid.k8s.apiClientConfig.watchReconnectLimit=-1
> druid.k8s.apiClientConfig.watchReconnectInterval=1000
> ```

---

### 6-6. Deep Storage: S3 (MinIO 호환)

```properties
druid.storage.type=s3
druid.s3.endpoint.url=http://192.168.0.100:9000
druid.s3.enablePathStyleAccess=true      # MinIO 필수
druid.s3.forceGlobalBucketAccessEnabled=false
```

MinIO를 S3 호환 스토리지로 사용합니다. AWS S3와의 차이점:

| 항목 | AWS S3 | MinIO |
|------|--------|-------|
| `endpoint.url` | 설정 불필요 (기본값) | 반드시 명시 |
| `enablePathStyleAccess` | `false` | **`true` 필수** |
| 인증 | IAM Role / 키 | 액세스 키 |

MinIO에 필요한 버킷: `druid` (segments + indexing-logs 공용)

---

### 6-7. Kustomize 구조 설계 원칙

#### base = 의존성 없는 최소 구성

`base/`는 MinIO, PVC, 외부 스토리지 없이 **바로 apply 가능한 완전한 구성**입니다.  
데이터는 pod 재시작 시 사라지지만(emptyDir), 기능 자체는 모두 동작합니다.  
개발 환경이나 기능 테스트에 적합합니다.

#### Components = 재사용 가능한 기능 블록

Kustomize의 `Component`(kind: Component)는 overlay와 달리  
**여러 overlay에서 공유할 수 있는 모듈**입니다.

```
# 예: Druid 38.0.0 overlay를 추가할 때
overlays/apache-druid-38.0.0/kustomization.yaml
  resources: [../../base]
  components:
    - ../../components/pvc                    ← 구조 변환만 (emptyDir → PVC)
  patches:
    - patches/pvc-patch.yaml                  ← StorageClass / 크기 (필요 시)
    - patches/druid-common-config-with-minio.yaml  ← 스토리지 자격증명
    - 38.0.0 전용 변경사항만
```

#### PVC와 스토리지를 분리한 이유

```
pvc 컴포넌트     → Kubernetes 레이어 (볼륨 구성)
storage 컴포넌트 → Druid 레이어 (애플리케이션 설정)
```

두 관심사가 독립적이기 때문에 분리했습니다.  
예: 로컬 PVC + HDFS 조합, emptyDir + S3 조합 모두 가능합니다.

---

## 7. 알려진 이슈

### 7-1. Task "Could not allocate segment" 실패

**현상**: Task 로그에 `Could not allocate segment` 에러와 함께 FAILED  
**원인**: 클러스터 재시작 직후 세그먼트 메타데이터가 아직 로드되지 않은 상태에서 Task가 실행된 경우  
**영향**: 이미 커밋된 세그먼트는 **삭제되지 않음** (Druid의 세그먼트는 불변)  
**해결**: 재시작 후 Coordinator가 완전히 초기화(보통 30~60초)된 뒤 Supervisor를 재시작하면 해결됨

### 7-2. Kubernetes Watch 만료로 Indexer 오프라인 인식 (해결됨)

**현상**: ~60분마다 Overlord가 Indexer를 오프라인으로 인식, Task 재배정 실패  
**원인**: Kubernetes watch connection이 만료되고 재연결에 실패하는 Druid 버그  
**해결**: `watchReconnectLimit=-1` (무한 재연결) + `watchReconnectInterval=1000ms` 설정으로 해결

현상 재발 시 Overlord rollout restart로 즉시 복구 가능합니다.

### 7-3. Overlord 재시작 시 완료된 Task 이력 소멸

**현상**: Overlord 재시작 후 UI의 Tasks 탭에서 이전 Task 목록이 모두 사라짐  
**원인**: `druid.indexer.storage.type=local` (기본값) — Task 이력을 Overlord 메모리에만 저장

```
druid.indexer.storage.type=local   ← Overlord 메모리에만 저장 (기본값)
druid.indexer.storage.type=metadata ← PostgreSQL에 영구 저장
```

| 상황 | local 타입 | metadata 타입 |
|------|-----------|--------------|
| Overlord 재시작 | Task 이력 즉시 소멸 | 유지됨 |
| 자동 만료 TTL | 없음 | 설정 가능 |
| 저장 위치 | Overlord JVM 힙 메모리 | PostgreSQL `druid_tasks` 테이블 |

**base 기본값**: `druid.indexer.storage.type=local` (Task 이력 미보존)

**해결**: overlay의 `patches/overlord-configmap-patch.yaml`이 `metadata`로 교체합니다.

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

`kustomization.yaml`의 `patches:`에 등록되어 있습니다:

```yaml
patches:
  - path: patches/overlord-configmap-patch.yaml  # Task 이력 저장: local → metadata (PostgreSQL)
```

#### `druid.indexer.storage.remote.maxTaskStatusCount`

`storage.type=metadata` 사용 시 Overlord UI / API에서 조회되는 **완료된 Task 이력 최대 건수**를 제한합니다.

| 항목 | 내용 |
|------|------|
| 기본값 | `10` |
| 적용 조건 | `storage.type=metadata` 일 때만 유효 |
| 영향 범위 | Overlord UI Tasks 탭, `/druid/indexer/v1/tasks` API 응답 건수 |
| DB 보관 | 이 값과 무관하게 PostgreSQL `druid_tasks` 테이블에는 전체 이력이 남음 |

기본값(10)은 최근 10건만 노출합니다. 이력을 더 많이 보고 싶다면 overlay 패치에 추가합니다:

```yaml
    druid.indexer.storage.type=metadata
    druid.indexer.storage.remote.maxTaskStatusCount=100  # 필요 시 추가
    druid.server.http.numThreads=60
```

> base에서는 이 설정을 제거했습니다. 기본값(10)으로 동작하며, 필요한 overlay에서만 명시합니다.

> **주의**: `runtime.properties` 키 전체를 교체하므로 기존 프로퍼티를 모두 포함해야 합니다.  
> 보관 건수를 늘리려면 `maxTaskStatusCount` 값을 수정하면 됩니다.

Task 로그 파일 자체(S3 업로드본)는 storage.type과 무관하게 보존됩니다.  
storage.type은 Overlord UI에서 보이는 **Task 메타데이터 이력**에만 영향을 줍니다.

### 7-4. Indexer 메모리 과다 할당 경고

**현상**: `kubectl top node`에서 node3 메모리 93% 할당으로 표시  
**원인**: Indexer replicas=2, requests=1Gi × 2 = 2Gi가 node3에 집중될 경우  
**영향**: 실제 사용량이 request 이하이면 OOM은 발생하지 않음 (Kubernetes 스케줄링 기준은 requests)  
**모니터링**: `kubectl top pod`, `kubectl describe node node3 | grep MemoryPressure`

### 7-5. Druid 37 + Jetty 12 — Router managementProxy 스레드 부족 (해결됨)

**현상**: Router Pod CrashLoopBackOff 또는 기동 직후 오류

```
Insufficient configured threads: required=2 < max=2 for AsyncManagementForwardingServlet
```

**배경**

Jetty가 Druid **35.0.0부터 12.x로 업그레이드**됐습니다 (34.0.0이 Jetty 9.x 마지막 버전).

| Druid 버전 | Jetty 버전 | managementProxy | K8s discovery |
|------------|-----------|----------------|---------------|
| 34.0.0 | 9.x | 정상 | 불안정 (watch stale event) |
| 35.0.0+ / 37.0.0 | 12.x | **버그** (기본) | 안정 |
| **37.0.0 + 워크어라운드** | **12.x** | **정상** | **안정** |

**원인**

Router의 `managementProxy`가 내부 HTTP client를 생성할 때 스레드 풀 크기를 아래처럼 계산합니다:

```
max = druid.router.http.numConnections / 25
```

기본값 `numConnections=50` → `max=2`. Jetty 12는 reserved thread 1개를 차감하므로  
실제 사용 가능 thread = 1개 → selector에 필요한 2개에 미달 → 오류 발생.

단일 CPU Pod(컨테이너)에서 `Runtime.availableProcessors() = 1`로 반환되면 Jetty 12가  
thread pool `max=2`로 계산하는 버그와 맞물려 재현됩니다.

**해결**: Router JVM에 `-XX:ActiveProcessorCount=4` 추가

```properties
# overlays/apache-druid-37.0.0/patches/jvm-heaps.yaml → router jvm.config
-server
-XX:ActiveProcessorCount=4   ← 추가: JVM이 4코어로 인식 → Jetty thread pool max=4+
-Xms256m
-Xmx256m
-XX:MaxDirectMemorySize=128m
```

`availableProcessors()=4` → `max = 4` → Jetty 12 reserved thread 1개 제외 후 3개 사용 가능 → 오류 해소.

**시도했던 임시 방법들** (비권장)

```properties
# 방법 A: managementProxy 비활성화 (Druid 웹 콘솔에서 Coordinator/Overlord API proxy 불가)
druid.router.managementProxy.enabled=false

# 방법 B: numConnections 직접 증가 (근본 원인인 availableProcessors 문제는 남음)
druid.router.http.numConnections=200   # 200/25 = 8 threads → 여유 확보
```

방법 A는 웹 콘솔에서 `Load data` 설정이 동작하지 않는 부작용이 있어  
`-XX:ActiveProcessorCount=4`를 최종 해결책으로 채택했습니다.

### 7-6. Control-plane(plane1) 메모리 증가 — apiserver watch 누적

> **주의**: Druid 배포 후 plane1(control-plane) 메모리가 꾸준히 증가하다가 일정 수준에서 안정됩니다. 이상 동작이 아닙니다.

**현상**

`druid.discovery.type=k8s` 설정으로 Druid 클러스터가 기동되면 plane1 메모리가 점진적으로 증가합니다.

**원인 — apiserver watch connection 누적**

Druid의 `druid-kubernetes-extensions`는 각 컴포넌트가 기동될 때 kube-apiserver에 watch connection을 열어 Pod 변화를 실시간으로 감지합니다. apiserver는 이 watch state를 메모리에 유지하기 때문에 Druid Pod 수에 비례해 plane1 메모리가 증가합니다.

```
Druid 컴포넌트 기동
  → druid-kubernetes-extensions 로드
  → kube-apiserver(plane1)에 Pod watch 등록
  → apiserver가 watch state를 메모리에 유지
  → plane1 메모리 증가 시작
```

**실제 확인 명령어**

```bash
kubectl get --raw /metrics \
  | grep 'apiserver_longrunning_requests' \
  | grep -v '^#' \
  | grep 'WATCH' \
  | grep -v '} 0' \
  | sort -t'}' -k2 -rn \
  | head -25
```

**실제 측정값 (Druid 37.0.0 운영 중)**

```
apiserver_longrunning_requests{resource="configmaps", scope="resource", verb="WATCH"} 41
apiserver_longrunning_requests{resource="services",   scope="cluster", verb="WATCH"} 25
apiserver_longrunning_requests{resource="pods",       scope="namespace",verb="WATCH"} 24  ← Druid 주요 기여
apiserver_longrunning_requests{resource="endpointslices",scope="cluster",verb="WATCH"} 18
apiserver_longrunning_requests{resource="nodes",      scope="cluster", verb="WATCH"} 17
apiserver_longrunning_requests{resource="namespaces", scope="cluster", verb="WATCH"} 17
apiserver_longrunning_requests{resource="pods",       scope="cluster", verb="WATCH"} 16
apiserver_longrunning_requests{resource="nodes",      scope="resource",verb="WATCH"} 15
apiserver_longrunning_requests{resource="ippools",    scope="cluster", verb="WATCH"} 10  ← Calico
```

**각 watch의 출처**

| 리소스 | 수 | 주요 사용처 |
|--------|-----|------------|
| `pods` namespace-scope | 24 | **Druid K8s discovery** (각 컴포넌트 Pod watch) |
| `pods` cluster-scope | 16 | kube-controller-manager, Calico |
| `configmaps` resource-scope | 41 | Druid + 시스템 컴포넌트 |
| `services`, `endpointslices` | 25, 18 | kube-proxy, Calico, MetalLB |
| `ippools`, `ipamblocks` 등 | 10, 6 | Calico CNI |
| `ipaddresspools`, `communities` 등 | 5 | MetalLB |

Druid 컴포넌트 6종(Coordinator, Overlord, Broker, Historical, Indexer×2, Router)이 각자 default namespace의 Pod를 watch하므로 `pods namespace-scope` 24개 중 상당 부분이 Druid 기여분입니다.

**메모리 증가 패턴**

```
배포 직후     → watch connection 순차 수립 → 메모리 점진적 증가
안정화 이후   → watch 수 고정 → 메모리 일정 수준 유지
Druid 재시작  → watch 재연결 → 일시적 증가 후 다시 안정
```

**이상 여부 판단 기준**

- 메모리가 증가하다가 **멈추면 정상**
- 메모리가 **계속 증가만 하면** watch connection이 재연결을 반복하는 상태 — `watchReconnectLimit=-1` 설정 확인 필요

```bash
# Druid가 Indexer를 정상 인식하는지 확인 (watch가 살아있는 증거)
kubectl exec druid-overlords-0 -- wget -qO- localhost:8090/druid/indexer/v1/workers \
  | python3 -m json.tool
```

worker 목록에 Indexer가 보이면 watch connection 정상 동작 중입니다.

---

## 8. 트러블슈팅 가이드

### 8-1. 기본 진단 명령어

#### Pod 상태 확인

```bash
# 전체 Pod 상태 (노드 배치, IP 포함)
kubectl get pod -o wide

# Ready 아닌 Pod 필터링
kubectl get pod | grep -v Running | grep -v Completed

# Pod 상세 — Events, 재시작 원인, 마운트 오류 등
kubectl describe pod <pod-name>

# 이전 컨테이너 로그 (CrashLoopBackOff일 때)
kubectl logs <pod-name> --previous

# 실시간 로그 스트림
kubectl logs <pod-name> -f --tail=100
```

#### 이벤트 확인

```bash
# 최근 Warning 이벤트 (OOMKilled, Evicted, FailedScheduling 등)
kubectl get events --field-selector type=Warning --sort-by='.lastTimestamp'

# 특정 Pod 관련 이벤트만
kubectl get events --field-selector involvedObject.name=<pod-name>
```

#### 리소스 사용량

```bash
# Pod별 실제 CPU/메모리 사용량
kubectl top pod --sort-by=memory

# 노드별 사용량
kubectl top node

# 노드 requests/limits 할당 현황 (93% 경고 등 확인)
kubectl describe node node1 node2 node3 | grep -A6 "Allocated resources"

# MemoryPressure 여부
kubectl describe node node1 node2 node3 | grep MemoryPressure
```

---

### 8-2. Druid 컴포넌트별 로그 확인

```bash
# 레이블로 한 번에 조회
kubectl logs -l nodeType=coordinator --tail=100
kubectl logs -l nodeType=overlord    --tail=100
kubectl logs -l nodeType=broker      --tail=100
kubectl logs -l nodeType=historical  --tail=100
kubectl logs -l nodeType=indexer     --tail=200   # Task 로그 포함
kubectl logs -l nodeType=router      --tail=100

# Indexer replicas=2일 때 각 Pod 별도 확인
kubectl logs druid-indexers-0 --tail=200
kubectl logs druid-indexers-1 --tail=200
```

#### Task 로그 직접 확인 (Indexer pod 내부)

```bash
# 실행 중이거나 완료된 Task 로그 파일 목록
kubectl exec druid-indexers-0 -- find /opt/druid/var/druid/task -name "log" -type f

# 특정 Task 로그 출력
kubectl exec druid-indexers-0 -- tail -200 /opt/druid/var/druid/task/slot0/<taskId>/log
```

---

### 8-3. Druid API로 내부 상태 확인

port-forward 후 curl로 각 컴포넌트의 내부 상태를 직접 조회합니다.

```bash
# Coordinator — 세그먼트 로드 상태
kubectl port-forward svc/druid-coordinators 8081:8081 &
curl -s http://localhost:8081/status/health
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus | python3 -m json.tool

# Overlord — Task 목록 및 상태
kubectl port-forward svc/druid-overlords 8090:8090 &
curl -s http://localhost:8090/druid/indexer/v1/tasks | python3 -m json.tool
curl -s "http://localhost:8090/druid/indexer/v1/task/<taskId>/status"

# Overlord — Worker(Indexer) 등록 현황
curl -s http://localhost:8090/druid/indexer/v1/workers | python3 -m json.tool

# Indexer — Worker 활성화 여부
kubectl port-forward svc/druid-indexers 8091:8091 &
curl -s http://localhost:8091/druid/worker/v1/enabled

# Historical — 로드된 세그먼트 목록
kubectl port-forward svc/druid-historicals 8083:8083 &
curl -s http://localhost:8083/druid/historical/v1/loadedSegments | python3 -m json.tool

# Router (UI API) — 전체 클러스터 헬스
kubectl port-forward svc/druid-routers 8888:8888 &
curl -s http://localhost:8888/status/health
curl -s http://localhost:8888/druid/v2/datasources
```

---

### 8-4. 실제 겪었던 오류와 해결

#### 오류 1: Tasks → Logs 버튼 클릭 시 빈 화면

**확인 방법**
```bash
# Indexer pod 내 task 디렉토리에 log 파일이 생성되는지 확인
kubectl exec druid-indexers-0 -- ls -la /opt/druid/var/druid/task/
```

**원인**  
Indexer는 Thread 방식이라 별도 log 파일이 자동 생성되지 않음.  
Druid UI는 `GET /druid/indexer/v1/task/{taskId}/log` 로 파일을 요청하는데, 파일이 없으면 빈 응답 반환.

**해결**: `base/indexer-configmap.yaml`의 `log4j2.xml`에 RoutingAppender 추가

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

Druid 소스 `Appenderators.setTaskThreadContextForIndexers()`가 Task 스레드 시작 시  
MDC 키 `task.log.id`와 `task.log.file`을 자동으로 설정하므로 이를 라우팅 기준으로 사용.

**RoutingAppender 적용 후에도 남는 WARN 로그**

RoutingAppender를 적용해도 아래 WARN이 Indexer 로그에 출력됩니다. **무해한 경고**입니다.

```
WARN [qtp755477196-91] org.apache.druid.indexing.worker.http.WorkerResource - Failed to read log for task: index_kafka_druid_6e4a4defbfb2f30_idnpaofk
java.io.FileNotFoundException: /opt/druid/var/druid/task/slot2/index_kafka_druid_.../log (No such file or directory)
```

**원인**: Overlord나 UI가 아직 실행 중이거나 막 시작된 Task의 로그를 미리 조회할 때, `createOnDemand="true"` 설정이므로 첫 로그 라인이 쓰이기 전까지 파일이 없어 FileNotFoundException이 발생합니다. Task가 실제로 로그를 기록하기 시작하면 파일이 생성되고 이후 조회는 정상 동작합니다.

**처리할 방법**: 별도 조치 불필요. 기능적으로 문제없으며 UI 로그 조회도 Task 실행 중에는 정상 동작합니다.

---

#### 오류 2: Overlord가 MiddleManager(Worker)를 ~60분마다 오프라인으로 인식

**확인 방법**
```bash
# Overlord 로그에서 worker 오프라인 메시지 검색
kubectl logs -l nodeType=overlord --tail=500 | grep -i "worker\|offline\|peon\|fork"

# Overlord API로 현재 worker 등록 현황 확인
kubectl port-forward svc/druid-overlords 8090:8090 &
curl -s http://localhost:8090/druid/indexer/v1/workers
```

**근본 원인**: MiddleManager 최초 기동 시 Kubernetes Discovery 라벨 패치 누락

Druid가 `druid.discovery.type=k8s`로 동작할 때, MiddleManager Pod는 기동 시  
`druidDiscoveryAnnouncement-cluster-identifier=druid-default` 라벨을 자신의 Pod에 패치합니다.  
이 라벨이 있어야 Overlord가 해당 Pod를 Worker로 인식합니다.

```
정상 기동 흐름:
  MiddleManager 기동 → Kubernetes API로 자신의 Pod에 라벨 패치 → Overlord watch 감지 → Worker 등록

문제 발생 흐름:
  MiddleManager 기동 → 라벨 패치 실패 또는 타이밍 누락
                     → Overlord가 watch에서 감지 못함 → worker=0으로 인식
                     → Task 배정 불가
```

**즉각 복구 방법 (MiddleManager 사용 시)**

```bash
# MiddleManager 재시작 → 라벨 재패치 → Overlord가 worker 재발견
kubectl rollout restart statefulset/druid-middlemanagers

# worker 등록 확인
curl -s http://localhost:8090/druid/indexer/v1/workers | python3 -m json.tool
```

**근본 해결**: MiddleManager를 제거하고 Indexer로 교체

Indexer는 `druidDiscoveryAnnouncement` 라벨 패치 방식 대신  
Kubernetes Service(`druid-indexers`)를 통해 Overlord가 직접 발견합니다.  
라벨 패치 실패 자체가 없어지므로 이 문제가 구조적으로 해소됩니다.

```
MiddleManager: Overlord → [K8s watch] → Pod 라벨 감지 → Worker 등록  (라벨 패치 필요)
Indexer:       Overlord → [K8s Service] → druid-indexers → Worker 등록 (라벨 패치 불필요)
```

추가로 `common-configmap.yaml`에 watch 재연결 설정을 넣어 간헐적 연결 만료도 방지합니다:
```properties
druid.k8s.apiClientConfig.watchReconnectLimit=-1
druid.k8s.apiClientConfig.watchReconnectInterval=1000
```

---

#### 오류 3: Task FAILED — "Could not allocate segment"

**확인 방법**
```bash
# Task 로그에서 원인 확인
kubectl exec druid-indexers-0 -- grep -r "Could not allocate" /opt/druid/var/druid/task/

# Coordinator 초기화 완료 여부 확인
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus
```

**원인**  
클러스터 재시작 직후 Coordinator가 메타데이터 DB에서 세그먼트 정보를 아직 다 읽지 못한 상태에서 Task가 실행됨.

**해결**  
재시작 후 30~60초 대기 후 Supervisor를 재시작:

```bash
# Supervisor 일시 중지 후 재개
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisorId>/suspend
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisorId>/resume
```

> 이미 커밋된 세그먼트는 삭제되지 않음. 실패한 Task만 재실행하면 됨.

---

#### 오류 4: MinIO(S3) 연결 실패 — "Access Denied" 또는 연결 거부

**확인 방법**
```bash
# Indexer 로그에서 S3 오류 검색
kubectl logs -l nodeType=indexer --tail=300 | grep -i "s3\|access\|endpoint"

# MinIO 연결 테스트 (Indexer pod 내부)
kubectl exec druid-indexers-0 -- curl -v http://<MINIO_HOST>:9000/druid
```

**원인 및 해결**

| 증상 | 원인 | 해결 |
|------|------|------|
| `Access Denied` | `enablePathStyleAccess=false` | `druid.s3.enablePathStyleAccess=true` 설정 |
| `Connection refused` | endpoint URL 미설정 | `druid.s3.endpoint.url=http://<MINIO_HOST>:9000` 명시 |
| 버킷 없음 | MinIO에 `druid` 버킷 미생성 | MinIO 콘솔에서 버킷 생성 |

```properties
# overlays/<your-cluster>/patches/druid-common-config-with-minio.yaml
druid.s3.endpoint.url=http://<MINIO_HOST>:9000
druid.s3.enablePathStyleAccess=true
druid.s3.forceGlobalBucketAccessEnabled=false
```

---

#### 오류 5: Kubernetes API Watch 만료로 Overlord가 Indexer를 간헐적으로 오프라인 인식

> MiddleManager → Indexer로 전환한 이후에도 간헐적으로 발생할 수 있는 별도 이슈입니다.

**확인 방법**
```bash
# Overlord 로그에서 watch 만료 메시지 검색
kubectl logs -l nodeType=overlord --tail=500 | grep -i "watch\|reconnect\|offline"
```

**원인**  
Druid가 Kubernetes API Server에 맺는 watch connection이 ~60분 후 서버 측에서 끊깁니다.  
Druid가 재연결에 실패하면 Overlord가 Indexer 상태를 갱신하지 못해 오프라인으로 판단합니다.

**해결**: `base/common-configmap.yaml`에 추가

```properties
druid.k8s.apiClientConfig.watchReconnectLimit=-1       # 무한 재연결 시도
druid.k8s.apiClientConfig.watchReconnectInterval=1000  # 1초 간격으로 재연결
```

---

#### 오류 6: OOMKilled 또는 노드 메모리 과부하

**확인 방법**
```bash
# OOMKilled Pod 확인
kubectl get pod -o json | python3 -c "
import json, sys
pods = json.load(sys.stdin)
for p in pods['items']:
    for cs in p.get('status', {}).get('containerStatuses', []):
        last = cs.get('lastState', {}).get('terminated', {})
        if last.get('reason') == 'OOMKilled':
            print(p['metadata']['name'], cs['name'], last)
"

# 노드별 할당 현황
kubectl describe node | grep -E "Name:|memory|cpu" | grep -A2 "Allocated"

# 실시간 사용량
kubectl top pod --sort-by=memory
```

**원인**: Kafka 기본 JVM heap 1GB + Indexer replicas=2(1Gi×2) = 노드 메모리 부족

**해결**: Kafka heap을 256MB로 낮춰 Indexer 2번째 Pod 배치 공간 확보

```yaml
# kafka StatefulSet env
- name: KAFKA_HEAP_OPTS
  value: "-Xmx256m -Xms256m"   # 기본 1GB → 256MB
```

실제 측정 결과: heap 256m에서 Kafka Pod 실제 사용량 ~78Mi (KRaft, 3 replica 기준).

---

### 8-5. 빠른 진단 스크립트

클러스터 전체 상태를 한 번에 출력합니다.

```bash
#!/bin/bash
echo "=== Pod 상태 ==="
kubectl get pod -o wide

echo ""
echo "=== Ready 아닌 Pod ==="
kubectl get pod | grep -Ev "Running|Completed|NAME"

echo ""
echo "=== 최근 Warning 이벤트 ==="
kubectl get events --field-selector type=Warning --sort-by='.lastTimestamp' | tail -15

echo ""
echo "=== 리소스 사용량 ==="
kubectl top pod --sort-by=memory 2>/dev/null || echo "(metrics-server 미설치)"

echo ""
echo "=== 노드 메모리 압박 ==="
kubectl describe node $(kubectl get node -o name | sed 's|node/||') \
  | grep -E "Name:|MemoryPressure"
```

---

## 9. Kafka 구성 참고

KRaft 모드 (ZooKeeper 불필요), 3 replicas. Headless Service로 Pod-to-Pod 통신을 구성합니다.

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
              value: "-Xmx256m -Xms256m"   # 기본 1GB → 256MB (Indexer replicas=2 메모리 확보)
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

### Cluster ID 생성 및 교체

`--cluster-id`는 KRaft 클러스터를 식별하는 **URL-safe Base64 인코딩된 UUID**(22자)입니다.  
모든 Pod의 initContainer와 env가 동일한 ID를 공유해야 합니다.

#### 언제 교체해야 하는가

| 상황 | 조치 |
|------|------|
| 신규 배포 | 새 ID 생성 후 yaml에 반영 |
| 데이터를 버리고 완전히 초기화할 때 | 새 ID 생성 + PVC 삭제 |
| 기존 클러스터 유지 (재배포, 설정 변경) | **ID 유지** (변경 시 기존 데이터 인식 불가) |

#### 새 Cluster ID 생성 방법

```bash
# 방법 1: Docker로 kafka-storage.sh 실용
docker run --rm apache/kafka:4.2.0 /opt/kafka/bin/kafka-storage.sh random-uuid

# 방법 2: Python (kafka-storage.sh와 동일한 포맷)
python3 -c "import uuid, base64; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode())"

# 방법 3: 이미 실행 중인 kafka Pod에서
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-storage.sh random-uuid
```

생성된 ID를 아래 **두 곳 모두** 교체합니다:

```yaml
# initContainer format-storage
/opt/kafka/bin/kafka-storage.sh format \
  --cluster-id <NEW_CLUSTER_ID> \    ← 여기
  ...

# container env
- name: CLUSTER_ID
  value: "<NEW_CLUSTER_ID>"           ← 여기
```

> ⚠️ ID 변경 시 기존 PVC 데이터를 재사용할 수 없습니다. PVC도 함께 삭제하고 재생성하세요:
> ```bash
> kubectl delete statefulset kafka
> kubectl delete pvc -l app=kafka
> kubectl apply -f 02-kafka-svc.yaml -f 05-kafka-stateful-set.yaml
> ```

### Supervisor 설정 (Druid에서 Kafka 연동)

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

## 10. Druid 아키텍처 — 컴포넌트 역할 및 데이터 흐름

### 10-1. 컴포넌트별 역할

```
┌─────────────────────────────────────────────────────────────────┐
│                        Query / Ingest                           │
│                                                                 │
│   Client ──→ Router ──→ Broker ──→ Historical                   │
│                    └──→ Overlord ──→ Indexer (실시간 세그먼트)   │
│                    └──→ Coordinator                             │
│                                                                 │
│                  [PostgreSQL: 메타데이터]                        │
│                  [S3/MinIO: Deep Storage]                       │
└─────────────────────────────────────────────────────────────────┘
```

| 컴포넌트 | 포트 | 역할 |
|----------|------|------|
| **Coordinator** | 8081 | 세그먼트 배포 관리. Historical에 "어떤 세그먼트를 로드/드롭할지" 지시. 보존 정책(Retention), 자동 컴팩션 실행 |
| **Overlord** | 8090 | Task 라이프사이클 관리. Supervisor 생성/종료, Task를 Indexer에 배정, Task 상태를 메타 DB에 기록 |
| **Broker** | 8082 | 쿼리 라우터. 세그먼트 타임라인을 보고 Historical·Indexer에 서브쿼리를 분산 후 결과를 머지해 반환 |
| **Historical** | 8083 | 불변 세그먼트 서빙. Deep Storage에서 세그먼트를 다운로드해 로컬에 캐시하고 쿼리에 응답 |
| **Indexer** | 8091 | 수집 Task 실행 (JVM Thread 방식). 신규 데이터를 실시간 인덱싱하고 세그먼트를 Deep Storage에 업로드 |
| **Router** | 8888 | API 게이트웨이 + 웹 콘솔. 쿼리는 Broker로, Task API는 Overlord로, 세그먼트 API는 Coordinator로 프록시 |

#### 컴포넌트 간 데이터 저장소

| 저장소 | 역할 | 이 배포에서 |
|--------|------|-------------|
| **Deep Storage** | 세그먼트 파일(.smoosh) 영구 보관 | S3/MinIO (`druid` 버킷) |
| **Metadata Store** | 세그먼트 등록 정보, Task 이력, Supervisor 상태 | PostgreSQL (`druid-postgresql:5432`) |
| **Local Cache** | Historical이 서빙을 위해 다운로드한 세그먼트 | PVC (`/opt/druid/var`) |

---

### 10-2. Kafka → Druid 수집 흐름

```
Kafka Topic
  │
  │  (1) Supervisor가 파티션 할당
  ▼
Indexer Task (실시간 소비)
  │  · Kafka Consumer로 메시지 읽기
  │  · 메모리 내 Incremental Index에 row 추가
  │  · taskDuration(기본 1h)마다 또는 행 수 임계치 도달 시 → Publish
  │
  │  (2) Publish 단계
  ├──▶ Deep Storage(S3)에 세그먼트 파일 업로드
  ├──▶ PostgreSQL 메타 DB에 세그먼트 등록
  └──▶ 새 Task가 다음 Kafka offset부터 이어서 소비
         │
         │  (3) Coordinator가 감지
         ▼
     Historical Node
       · S3에서 세그먼트 다운로드 → 로컬 캐시
       · 이후 쿼리는 Historical이 응답
```

#### Supervisor 상태 머신

```
PENDING ──▶ RUNNING ──▶ SUSPENDED
                │             │
                ▼             ▼
            STOPPING      RUNNING (resume 시)
                │
                ▼
           UNHEALTHY (Task 연속 실패 시)
```

```bash
# Supervisor 목록 및 상태 확인
curl -s http://localhost:8090/druid/indexer/v1/supervisor | python3 -m json.tool

# 특정 Supervisor 상세 상태
curl -s http://localhost:8090/druid/indexer/v1/supervisor/<id>/status

# Kafka offset 소비 현황 (각 파티션별 lag)
curl -s http://localhost:8090/druid/indexer/v1/supervisor/<id>/stats
```

#### Supervisor 파라미터: taskCount vs replicas

두 파라미터는 **서로 다른 목적**을 가집니다.

| 파라미터 | 역할 | 효과 |
|----------|------|------|
| `taskCount` | 파티션을 나눠서 처리 | **처리량 ↑** |
| `replicas` | 동일한 데이터를 중복 처리 | **고가용성(HA) ↑** |

**설정 조합별 동작**

| 설정 | Task 수 | 파티션 분배 | 효과 |
|------|---------|------------|------|
| `taskCount=1, replicas=2` (현재) | 2 | 각 Task가 P0,P1,P2 전체 소비 | HA (장애 내성) |
| `taskCount=2, replicas=1` | 2 | Task-A → P0,P1 / Task-B → P2 | 처리량 2배 |
| `taskCount=2, replicas=2` | 4 | 위 구성에 각각 복제본 추가 | HA + 처리량 2배 |

**현재 구성 (taskCount=1, replicas=2) — HA 동작**

```
Kafka Topic (partition 0, 1, 2)
           │
     ┌─────┴─────┐
     │  Overlord  │  ← Supervisor가 Task 2개 생성/관리
     └─────┬─────┘
     ┌─────┴──────────────────┐
     ▼                        ▼
  Task-A                   Task-B
(P0, P1, P2 전체 소비)   (P0, P1, P2 전체 소비)
     │                        │
druid-indexers-0         druid-indexers-1

Task-A 장애 발생
  → Task-B가 마지막 커밋 offset부터 즉시 재개 (데이터 유실 없음)
  → Supervisor가 druid-indexers-0에 새 Task-A 재배정
  → 두 Task 다시 동시 소비 (HA 복구)
```

**처리량 확장 시 (taskCount=2, replicas=1)**

```
Kafka Topic (partition 0, 1, 2)
           │
     ┌─────┴─────┐
     │  Overlord  │  ← Supervisor가 Task 2개 생성
     └─────┬─────┘
     ┌─────┴──────────────────┐
     ▼                        ▼
  Task-A                   Task-B
(P0, P1 담당)             (P2 담당)
     │                        │
druid-indexers-0         druid-indexers-1
```

> `taskCount`는 Kafka 파티션 수를 초과할 수 없습니다. 파티션=3이면 `taskCount` 최대 3.

```json
// 처리량 + HA 동시 적용 예시 (파티션 3개, Task 6개)
"taskCount": 3,
"replicas": 2
```

#### Supervisor Task 상태: active vs publishing

Supervisor 화면에서 아래와 같은 상태를 동시에 볼 수 있습니다:

```
(1 task × 2 replicas)
2 active tasks
2 publishing tasks
```

두 상태는 **서로 다른 세대(generation)의 task**입니다.

| 상태 | 하는 일 | Kafka 읽기 | deep storage 업로드 |
|------|---------|-----------|-------------------|
| **active** | Kafka 파티션 실시간 소비 중 | ✅ 계속 읽음 | ❌ |
| **publishing** | 세그먼트 finalize/push 중 | ❌ 읽기 종료 | ✅ 진행 중 |

**task 생명주기:**

```
[active]  Kafka에서 데이터 읽는 중
    ↓  segment granularity 경계 도달 or task.duration 만료
[publishing]  Kafka 읽기 중단 → 세그먼트 병합 → deep storage push → metadata 등록
    ↓  완료
[종료]  Historical이 세그먼트 로드 → 쿼리 가능
```

active task가 segment 경계에 도달하면:
1. 해당 task는 **publishing** 상태로 전환 (S3/MinIO에 push 시작)
2. Supervisor가 **새 active task를 즉시 생성** — Kafka 읽기 공백 없음
3. publishing task가 완료되면 Historical이 세그먼트를 로드

즉 `2 active + 2 publishing`은 **정상 상태**입니다. 이전 세대가 push를 마무리하는 동안 새 세대가 이미 Kafka를 읽고 있는 것입니다.

---

### 10-3. 쿼리 실행 흐름

```
Client (SQL / Native JSON)
  │
  ▼
Router :8888
  │  · 쿼리 → Broker로 프록시
  │  · Task API → Overlord로 프록시
  │
  ▼
Broker :8082
  │  (1) 세그먼트 타임라인 조회
  │      · Coordinator로부터 "어떤 Historical이 어떤 세그먼트를 갖는지" 캐시
  │      · Indexer로부터 "실시간 세그먼트 범위" 캐시
  │
  │  (2) 서브쿼리 분산
  ├──▶ Historical :8083  (published 세그먼트)
  └──▶ Indexer :8091    (아직 publish 안 된 실시간 세그먼트)
         │
         │  (3) 각 노드가 담당 세그먼트에서 부분 결과 반환
         ▼
Broker (4) 부분 결과 머지 → 최종 집계/정렬/LIMIT 적용
  │
  ▼
Client ← 최종 응답
```

#### 쿼리 시 세그먼트 선택 기준

```
쿼리: SELECT ... WHERE __time BETWEEN '2025-01-01' AND '2025-01-02'

Broker가 확인하는 것:
  1. 해당 시간 범위를 커버하는 세그먼트 목록 (메타 캐시)
  2. 각 세그먼트가 어느 Historical/Indexer에 있는지
  3. 중복 세그먼트가 있으면 우선순위 높은 버전만 선택 (compaction 결과 우선)
```

#### 쿼리 성능 영향 요소

| 요소 | 영향 | 튜닝 포인트 |
|------|------|-------------|
| Historical 수 | 세그먼트 병렬 처리 | replicas, 노드 추가 |
| Broker heap | 머지 시 메모리 | `-Xmx` 증가 |
| 세그먼트 크기 | 파일 I/O | 컴팩션으로 병합 |
| 세그먼트 캐시 | S3 재다운로드 방지 | Historical PVC 크기 |
| Broker 쿼리 캐시 | 반복 쿼리 가속 | `druid.broker.cache.*` |

```bash
# 현재 로드된 세그먼트 수 및 크기 확인
curl -s http://localhost:8081/druid/coordinator/v1/datasources/<datasource>/segments \
  | python3 -c "import json,sys; segs=json.load(sys.stdin); print(f'총 {len(segs)} 세그먼트')"

# Broker의 세그먼트 타임라인 캐시 확인
curl -s http://localhost:8082/druid/broker/v1/loadstatus

# 쿼리 실행 계획 확인 (SQL)
curl -s -X POST http://localhost:8082/druid/v2/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "EXPLAIN PLAN FOR SELECT COUNT(*) FROM <datasource>"}' \
  | python3 -m json.tool
```

---

### 10-4. Coordinator — 세그먼트 생명주기

```
Indexer가 세그먼트 업로드
  │
  ▼
PostgreSQL 메타 DB에 등록 (used=1)
  │
  ▼
Coordinator가 감지 (polling 주기: druid.coordinator.period=PT30S)
  │
  ├──▶ 보존 정책(Retention) 확인
  │      · 기간 초과 세그먼트 → used=0 (논리 삭제)
  │      · Deep Storage 물리 삭제는 별도 Kill Task가 수행
  │
  ├──▶ 복제 규칙 확인
  │      · replicants=1 → Historical 1개에만 로드
  │      · replicants=2 → Historical 2개에 복제
  │
  └──▶ 로드 지시
         Historical에 "이 세그먼트를 S3에서 다운로드해서 서빙하라"
```

```bash
# 세그먼트 로드 진행률
curl -s http://localhost:8081/druid/coordinator/v1/loadstatus

# 미로드 세그먼트 목록 (Historical이 아직 받지 못한 것)
curl -s http://localhost:8081/druid/coordinator/v1/loadqueue

# 세그먼트 삭제 (논리 삭제)
curl -X DELETE "http://localhost:8081/druid/coordinator/v1/datasources/<datasource>/intervals/<interval>"
```

---

### 10-5. MiddleManager vs Indexer — 수집 흐름 비교

#### MiddleManager 방식 (이전 구성)

```
[Router UI] Load data 설정
    ↓
[Router] managementProxy → Overlord API 전달
    ↓
[Overlord] Supervisor 등록 및 상시 실행
    ↓
[Overlord] taskDuration마다 Task 생성 결정 → MiddleManager에 Task 배정 요청
    ↓
[MiddleManager] Task 수신 → Peon 프로세스 fork (별도 JVM 실행)
    ↓
[Peon JVM] Kafka consuming → 메모리 버퍼링 → 세그먼트 생성 → S3 업로드
    ↓
[Overlord] Supervisor가 taskDuration 만료 감지
    ├── Peon에게 중지 신호
    └── MiddleManager에 새 Task 배정 → 새 Peon fork
    ↓
[Peon] S3 업로드 완료 → PostgreSQL에 세그먼트 메타데이터 기록
    ↓
[Coordinator] PostgreSQL 폴링으로 새 세그먼트 감지 (30초 주기)
    ↓
[Historical] S3에서 세그먼트 다운로드 → 로컬 PVC 캐시 → 쿼리 대기
```

#### Indexer 방식 (현재 구성)

```
[Router UI] Load data 설정
    ↓
[Router] managementProxy → Overlord API 전달
    ↓
[Overlord] Supervisor 등록 및 상시 실행
    ↓
[Overlord] taskDuration마다 Task 생성 결정 → Indexer에 Task 배정 요청
    ↓
[Indexer JVM] Task를 JVM 스레드로 실행 (fork 없음)
              └── MDC 키 설정 → log4j2 RoutingAppender → 파일에 로그 기록
    ↓
[Indexer Thread] Kafka consuming → 메모리 버퍼링 → 세그먼트 생성 → S3 업로드
    ↓
[Overlord] Supervisor가 taskDuration 만료 감지
    ├── Thread에 중지 신호 (인터럽트)
    └── 새 Task 스레드 시작 (새 fork 불필요)
    ↓
[Indexer] S3 업로드 완료 → PostgreSQL에 세그먼트 메타데이터 기록
    ↓
[Coordinator] PostgreSQL 폴링으로 새 세그먼트 감지 (30초 주기)
    ↓
[Historical] S3에서 세그먼트 다운로드 → 로컬 PVC 캐시 → 쿼리 대기
```

#### 방식별 차이점

| 구분 | MiddleManager | Indexer (현재) |
|------|--------------|----------------|
| Task 실행 방식 | Peon 별도 프로세스 (fork) | 같은 JVM 내 스레드 |
| 로그 파일 | 프로세스별 자동 생성 → S3 업로드 | RoutingAppender 설정 필요 |
| UI 로그 조회 | 기본 동작 | RoutingAppender 적용 후 동작 |
| K8s 리소스 모델 | Pod 단위 격리와 불일치 | Pod = JVM = Task 그룹, 일치 |
| Worker 등록 방식 | Pod 라벨 패치 (`druidDiscoveryAnnouncement-*`) | Kubernetes Service 기반 발견 |
| 장애 시 Task | Pod 재시작 = 모든 Peon 강제 종료 | 스레드 종료, JVM은 유지 |
| `worker.capacity` | MiddleManager당 동시 Peon 수 | Indexer당 동시 Task 스레드 수 |

#### worker.capacity 설정

```properties
# base/indexer-configmap.yaml → runtime.properties
druid.worker.capacity=3   # 한 Indexer Pod이 동시에 처리할 수 있는 Task 수
```

`replicas=2` (Indexer Pod 2개) + `worker.capacity=3` = 클러스터 전체 최대 6 Task 동시 실행.  
`worker.capacity`를 늘리면 JVM이 모든 Task를 함께 수용해야 하므로 `-Xmx`도 함께 늘려야 합니다.

---

## 참고 링크

- [Apache Druid 37.0.0 문서](https://druid.apache.org/docs/37.0.0/)
- [Druid Kubernetes Extensions 설정](https://druid.apache.org/docs/latest/development/extensions-core/kubernetes)
- [Druid Task Logging](https://druid.apache.org/docs/latest/configuration/#task-logging)
- [Druid Extensions 목록](https://druid.apache.org/docs/latest/configuration/extensions)
- [Kustomize Components](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/components/)
