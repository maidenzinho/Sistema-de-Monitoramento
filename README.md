# Sistema de monitoramento

![Security Stack](https://img.shields.io/badge/security-stack-blue)
![Status](https://img.shields.io/badge/status-lab--ready-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

Guia prático para montar uma stack de monitoramento de rede usando **Suricata + Filebeat + Elasticsearch + Grafana**.

A proposta é simples:

- instalar **Suricata** nos servidores monitorados
- usar **Filebeat** para enviar os logs
- armazenar tudo em um **Elasticsearch central**
- visualizar os eventos no **Grafana**

Esse modelo é muito usado em:

- laboratórios de segurança
- estudos de IDS/IPS
- ambientes de teste
- homelabs
- pequenas infraestruturas

Este guia foca em uma implementação **simples e funcional**, sem depender de ferramentas mais complexas como Elastic Security ou SIEM completos.

---

# Sumário

- Arquitetura
- Pré-requisitos
- Instalação do Elasticsearch
- Instalação do Grafana
- Instalação do Suricata
- Instalação do Filebeat
- Validação dos logs
- Configuração do Grafana
- Problemas comuns
- Boas práticas de segurança
- Estrutura do projeto

---

# Arquitetura

Fluxo de dados da stack:

Servidor Monitorado

Suricata  
↓  
gera logs em:

/var/log/suricata/eve.json

↓

Filebeat lê o arquivo e envia os eventos

↓

Servidor Central

Elasticsearch  
armazenamento e indexação dos logs

↓

Grafana  
visualização e dashboards

Opcionalmente você pode usar **Kibana** para inspecionar diretamente os documentos do Elasticsearch.

---

# Pré-requisitos

## Sistemas recomendados

Testado em:

- Ubuntu 22.04
- Ubuntu 24.04

Outras distribuições podem funcionar, mas os comandos podem variar.

---

## Recursos mínimos

Servidor central:

2 vCPU  
4 GB RAM  
20 GB de disco

Servidores monitorados:

1 vCPU  
1 GB RAM

---

## Portas utilizadas

| Serviço | Porta |
|------|------|
| Elasticsearch | 9200 |
| Grafana | 3000 |

Os agentes precisam acessar o Elasticsearch pela porta **9200**.

---

# Checklist inicial

Antes de começar a instalação, verifique alguns pontos importantes.

## Sincronização de horário

Todos os servidores devem ter horário sincronizado.

Execute:

timedatectl

O resultado deve mostrar:

System clock synchronized: yes

---

## Definir hostname

Definir um hostname ajuda a identificar cada servidor dentro do Grafana.

Exemplo:

hostnamectl set-hostname web01

---

# 1) Instalar Elasticsearch (Servidor Central)

Instalar dependências:

sudo apt-get update  
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg

Adicionar repositório:

curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

Instalar:

sudo apt-get update  
sudo apt-get install -y elasticsearch

Habilitar serviço:

sudo systemctl enable --now elasticsearch

---

## Configuração básica

Editar arquivo:

sudo nano /etc/elasticsearch/elasticsearch.yml

Configuração mínima:

cluster.name: suricata-central  
node.name: es01  
network.host: 0.0.0.0  
http.port: 9200  
discovery.type: single-node

Reiniciar:

sudo systemctl restart elasticsearch

Testar:

curl -k https://localhost:9200

---

# 2) Instalar Grafana (Servidor Central)

Instalar dependências:

sudo apt-get install -y adduser libfontconfig1

Adicionar repositório:

sudo mkdir -p /etc/apt/keyrings

curl -fsSL https://apt.grafana.com/gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/grafana.gpg

echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list

Instalar Grafana:

sudo apt-get update  
sudo apt-get install -y grafana

Iniciar serviço:

sudo systemctl enable --now grafana-server

Acessar:

http://IP_DO_SERVIDOR:3000

Login inicial:

admin / admin

---

# 3) Instalar Suricata (Servidores Monitorados)

Instalar:

sudo apt-get install -y software-properties-common

sudo add-apt-repository ppa:oisf/suricata-stable

sudo apt-get update  
sudo apt-get install -y suricata

Ativar serviço:

sudo systemctl enable --now suricata

---

## Descobrir interface de rede

ip route

Interfaces comuns:

eth0  
ens18  
eno1

---

## Configurar captura de tráfego

Editar:

sudo nano /etc/suricata/suricata.yaml

Configurar seção af-packet:

af-packet:
  - interface: eth0
    cluster-id: 99
    cluster-type: cluster_flow

---

## Atualizar regras

sudo suricata-update

Testar configuração:

sudo suricata -T -c /etc/suricata/suricata.yaml -v

Reiniciar:

sudo systemctl restart suricata

---

# 4) Instalar Filebeat

Instalar:

sudo apt-get update  
sudo apt-get install -y filebeat

---

## Habilitar módulo Suricata

sudo filebeat modules enable suricata

Editar:

sudo nano /etc/filebeat/modules.d/suricata.yml

Configuração:

- module: suricata
  eve:
    enabled: true
    var.paths: ["/var/log/suricata/eve.json"]

---

# 5) Configurar envio para Elasticsearch

Editar:

sudo nano /etc/filebeat/filebeat.yml

Exemplo:

output.elasticsearch:
  hosts: ["https://IP_DO_ELASTIC:9200"]
  username: "filebeat_ingest"
  password: "senha_aqui"

---

## Identificar servidor nos logs

Adicionar:

processors:
  - add_fields:
      target: ''
      fields:
        sensor_name: "web01"

---

# 6) Iniciar Filebeat

Testar:

sudo filebeat test config

Iniciar:

sudo systemctl enable --now filebeat

Ver logs:

sudo journalctl -u filebeat -n 50

---

# 7) Validar ingestão de logs

No Elasticsearch:

GET filebeat-*/_search

Se aparecerem documentos com `@timestamp`, a ingestão está funcionando.

---

# Configurar Grafana

Adicionar datasource:

Connections  
→ Data sources  
→ Elasticsearch

Configurar:

URL:

https://IP_DO_ELASTIC:9200

Index pattern:

filebeat-*

Time field:

@timestamp

---

# Problemas comuns

Filebeat não consegue ler eve.json:

Verificar permissões:

ls -l /var/log/suricata/eve.json

Corrigir com:

sudo setfacl -m u:filebeat:r /var/log/suricata/eve.json

---

Certificado TLS inválido:

Para laboratório pode usar:

ssl.verification_mode: none

Não recomendado em produção.

---

# Boas práticas de segurança

- não usar usuário elastic diretamente nos agentes
- preferir autenticação via API Key
- restringir acesso ao Elasticsearch via firewall
- usar TLS entre agentes e servidor central
- revisar permissões de leitura dos logs

---

# Estrutura sugerida do repositório

docs/  
documentação adicional

templates/  
arquivos de configuração exemplo

scripts/  
scripts de instalação

dashboards/  
modelos de dashboards Grafana

---

Fim do guia.