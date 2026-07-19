# Deploy — agente-advisor (Railway)

## Passos (na pasta app/ deste projeto)
```bash
railway init          # criar projeto "agente-advisor"
railway up            # deploy
```

## Variáveis de ambiente (Railway → Variables)
| Variável | Valor |
|---|---|
| LLM_GATEWAY_URL | URL do mana-llm-gateway (ex.: https://mana-llm-gateway-production.up.railway.app) |
| LLM_GATEWAY_KEY | chave virtual criada no cockpit para o agente-advisor |
| LLM_MODEL | claude-sonnet-4-5 (ou alias mana-equilibrio) |
| CRON_HORA | 7 (hora BRT do processamento diário; opcional) |
| TZ | America/Sao_Paulo |

## Observações
- IMPORTANTE (padrão Maná): criar chave virtual própria "agente-advisor" no cockpit do gateway para rastrear custo.
- workers=1 no gunicorn é obrigatório (APScheduler + progresso em memória).
- Estado do pipeline é o filesystem (data/). Railway tem disco efêmero: a cada redeploy o app volta ao estado do repositório. Para os 6 vídeos atuais isso é ok (estão commitados em data/). Quando o volume crescer: anexar um Railway Volume em /app/data ou migrar para o PG banco-mana (schema advisor) — ver ARQUITETURA.md.
- RISCO CONHECIDO: a extração de legendas pode ser bloqueada pelo YouTube em IP de datacenter. Se o botão "Processar" der erro "sem legenda/bloqueou", o plano B é capturar as transcrições via sessão Cowork (Chrome) e colocar os .txt em data/transcricoes/ — o restante do pipeline segue normal.
- WhatsApp (fase futura): inbound via agente-router (keyword "advisor"), outbound via hub agente-whatsapp — não incluído nesta versão.
