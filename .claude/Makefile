CLAUDE_IMAGE ?= claude-cli:$(AGENT_PROJECT_ID)

claude: _codex_volume_create agent-vpn-check ## codex
	docker build \
	  --build-arg UID=$(USER) \
	  --build-arg GO_VERSION=1.27 \
	  --build-arg WORKDIR=/app \
	  --target cli \
	  -t "$(CLAUDE_IMAGE)" .codex
	@rc=0; \
	docker run --init --rm -it \
	  -u "$(USER)" \
	  --network "container:$(AGENT_VPN_CONTAINER)" \
	  --mount "type=bind,src=$(AGENT_VPN_RESOLV_CONF),dst=/etc/resolv.conf,readonly" \
	  --name claude_gokafka \
	  --env-file .codex/local.env \
	  -v "claude_gokafka_data:/home/developer" \
	  -v "$$(pwd):/app" \
	  -w /app \
	  "$(CLAUDE_IMAGE)" || rc=$$?; \
	docker image rm -f "$(CLAUDE_IMAGE)" >/dev/null 2>&1 || true; \
	exit $$rc

aherdr:
	HERDR_AGENT=claude docker exec -it \
      -u $(USER) \
      -w /app \
      claude_gokafka \
      claude

claude_backup: _claude_store_create _claude_store_backup _claude_store_remove

claude_restore: _claude_store_create _claude_store_restore _claude_store_remove

_claude_volume_create:
	docker volume create claude_gokafka_data

_claude_volume_remove:
	docker volume rm -f claude_gokafka_data

_claude_store_create:
	docker run -u $(USER) -v claude_gokafka_data:/data --name claude_store alpine sh

_claude_store_remove:
	docker rm -f claude_store

_claude_store_backup:
	docker run --rm --volumes-from claude_store \
		-u $(USER) \
		-v "$$(pwd)/.claude/backup:/backup" \
		alpine tar czvf /backup/claude.tar.gz data/

_claude_store_restore:
	docker run --rm --volumes-from claude_store \
		-u $(USER) \
		-v "$$(pwd)/.claude/backup:/backup" \
		alpine sh -c "cd /data && rm -rf /data && tar xzvf /backup/claude.tar.gz --strip-components 1"
