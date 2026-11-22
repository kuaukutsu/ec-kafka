CODEX_IMAGE ?= codex-cli:$(AGENT_PROJECT_ID)

codex: _codex_volume_create agent-vpn-check ## codex
	docker build \
	  --build-arg UID=$(USER) \
	  --build-arg GO_VERSION=1.27 \
	  --build-arg WORKDIR=/app \
	  --target cli \
	  -t "$(CODEX_IMAGE)" .codex
	@rc=0; \
	docker run --init --rm -it \
	  -u "$(USER)" \
	  --network "container:$(AGENT_VPN_CONTAINER)" \
	  --mount "type=bind,src=$(AGENT_VPN_RESOLV_CONF),dst=/etc/resolv.conf,readonly" \
	  --name codex_gokafka \
	  --env-file .codex/local.env \
	  -v "codex_gokafka_data:/home/developer" \
	  -v "$$(pwd):/app" \
	  -w /app \
	  "$(CODEX_IMAGE)" || rc=$$?; \
	docker image rm -f "$(CODEX_IMAGE)" >/dev/null 2>&1 || true; \
	exit $$rc
	
oherdr:
	HERDR_AGENT=codex docker exec -it \
      -u $(USER) \
      -w /app \
      codex_gokafka \
      codex

codex_backup: _codex_store_create _codex_store_backup _codex_store_remove

codex_restore: _codex_store_create _codex_store_restore _codex_store_remove

_codex_volume_create:
	docker volume create codex_gokafka_data

_codex_volume_remove:
	docker volume rm -f codex_gokafka_data

_codex_store_create:
	docker run -u $(USER) -v codex_gokafka_data:/data --name codex_store alpine sh

_codex_store_remove:
	docker rm -f codex_store

_codex_store_backup:
	docker run --rm --volumes-from codex_store \
		-u $(USER) \
		-v "$$(pwd)/.codex/backup:/backup" \
		alpine tar czvf /backup/codex.tar.gz data/

_codex_store_restore:
	docker run --rm --volumes-from codex_store \
		-u $(USER) \
		-v "$$(pwd)/.codex/backup:/backup" \
		alpine sh -c "cd /data && rm -rf /data && tar xzvf /backup/codex.tar.gz --strip-components 1"
