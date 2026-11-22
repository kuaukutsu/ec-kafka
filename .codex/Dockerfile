ARG GO_VERSION=1.27

FROM ghcr.io/kuaukutsu/agent-skills:codex-go${GO_VERSION} AS build

# Arguments
ARG UID=10001
ARG WORKDIR="/src"

# user
# Отключаем перезапись PS1 в скопированном из /etc/skel/.bashrc
RUN <<EOF
    useradd -u ${UID} -s /bin/sh -m developer
    sed -i 's/^\([[:space:]]*\)PS1=/\1: # PS1=/' /home/developer/.bashrc
EOF
# workdir
RUN <<EOF
    mkdir -p ${WORKDIR}
    chown -R developer:developer ${WORKDIR}
EOF

FROM build AS cli

RUN mkdir -p /go/pkg/mod /go/bin && chown -R developer /go    

USER $UID
WORKDIR $WORKDIR

RUN <<EOF
  git config --global user.name "Dmitriy Krivopalov"
  git config --global user.email "dimarik82@gmail.com"
EOF

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash", "-l"]
