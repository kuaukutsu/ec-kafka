AGENT_VPN_CONTAINER ?= agent-vpn
AGENT_VPN_RESOLV_CONF ?= $(HOME)/.config/agent-vpn/resolv.conf
AGENT_PROJECT_ID := $(shell printf '%s' "$(CURDIR)" | sha256sum | cut -c1-12)

agent_vpn_check: ## Check the shared VPN before starting an agent
	@test -r "$(AGENT_VPN_RESOLV_CONF)" && test -s "$(AGENT_VPN_RESOLV_CONF)" || \
	  { echo "Missing or unreadable VPN DNS file: $(AGENT_VPN_RESOLV_CONF). Run make vpn-up in the VPN repository."; exit 1; }
	@state=$$(docker inspect -f '{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$(AGENT_VPN_CONTAINER)" 2>/dev/null) || \
	  { echo "VPN container not found. Run make vpn-up in the VPN repository."; exit 1; }; \
	[ "$$state" = 'true healthy' ] || \
	  { echo "VPN not ready ($$state). Check make vpn-status / vpn-logs in the VPN repository."; exit 1; }; \
	actual=$$(docker exec "$(AGENT_VPN_CONTAINER)" cat /run/agent-vpn/resolv.conf) || exit 1; \
	local_dns=$$(cat "$(AGENT_VPN_RESOLV_CONF)") || exit 1; \
	[ "$$actual" = "$$local_dns" ] || \
	  { echo "VPN DNS file is stale. Run make vpn-up in the VPN repository."; exit 1; }