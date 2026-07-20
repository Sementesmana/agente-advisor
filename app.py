# -*- coding: utf-8 -*-
"""agente-advisor — Advisor Alfredo Soares (Sementes Maná)
Pipeline: coletar transcrição YouTube -> sintetizar (Claude via mana-llm-gateway) -> consolidar mente -> chat persona.
Estado derivado do filesystem (data/): transcricoes/{id}.txt, sinteses/{id}.md, mente/{tema}.md, consolidado.json
"""
import os, io, json, re, glob, threading, traceback
import requests
from flask import Flask, request, jsonify, Response

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, 'data')
GW_URL = os.environ.get('LLM_GATEWAY_URL', '').rstrip('/')
GW_KEY = os.environ.get('LLM_GATEWAY_KEY', '')
MODEL = os.environ.get('LLM_MODEL', 'claude-sonnet-4-5')
CRON_HORA = int(os.environ.get('CRON_HORA', '7'))  # BRT
YT_CHANNEL_ID = os.environ.get('YT_CHANNEL_ID', 'UCh9HMS4C3F02msM-kiilAdA')  # @canaldoalfredosoares
TAXONOMIA = ['modelo-de-negocio','vendas-e-ofertas','marketing-de-influencia','canais-e-varejo',
             'conteudo-e-audiencia','gestao-e-pessoas','mentalidade-empreendedora',
             'networking-e-conexoes','branding-e-posicionamento']

app = Flask(__name__)
PROGRESSO = {'rodando': False, 'log': []}

def p(*a): return os.path.join(DATA, *a)
def ler(path): return open(path, encoding='utf-8').read() if os.path.exists(path) else ''
def gravar(path, txt):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, 'w', encoding='utf-8').write(txt)

def videos():
    vs = json.loads(ler(p('videos.json')) or '[]')
    ign = set(json.loads(ler(p('ignorados.json')) or '[]'))
    vs = [v for v in vs if v['id'] not in ign]
    cons = set(json.loads(ler(p('consolidado.json')) or '[]'))
    for v in vs:
        if v['id'] in cons: v['status'] = 'consolidado'
        elif os.path.exists(p('sinteses', v['id'] + '.md')): v['status'] = 'sintetizado'
        elif os.path.exists(p('transcricoes', v['id'] + '.txt')): v['status'] = 'transcrito'
        else: v['status'] = 'pendente'
    return vs

def log(msg):
    PROGRESSO['log'].append(msg)
    PROGRESSO['log'] = PROGRESSO['log'][-80:]
    print('[pipeline]', msg, flush=True)

# ---------- LLM (mana-llm-gateway, API compatível Anthropic /v1/messages) ----------
def _gw_headers():
    return {'x-api-key': GW_KEY, 'Authorization': 'Bearer ' + GW_KEY,
            'anthropic-version': '2023-06-01', 'content-type': 'application/json'}

def llm(system, user, max_tokens=8000):
    r = requests.post(GW_URL + '/v1/messages',
        headers=_gw_headers(),
        json={'model': MODEL, 'max_tokens': max_tokens, 'system': system,
              'messages': [{'role': 'user', 'content': user}]}, timeout=300)
    r.raise_for_status()
    return ''.join(b.get('text', '') for b in r.json().get('content', []))

# ---------- 1 COLETOR (determinístico) ----------
import html as _html

def ignorar(vid, motivo):
    ign = json.loads(ler(p('ignorados.json')) or '[]')
    if vid not in ign:
        ign.append(vid); gravar(p('ignorados.json'), json.dumps(ign))
    log('Ignorado %s (%s)' % (vid, motivo))

def coletar():
    """Busca os vídeos mais recentes do canal via RSS oficial e adiciona os novos ao topo da fila."""
    r = requests.get('https://www.youtube.com/feeds/videos.xml?channel_id=' + YT_CHANNEL_ID,
                     timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    entradas = re.findall(r'<entry>([\s\S]*?)</entry>', r.text)
    vs = json.loads(ler(p('videos.json')) or '[]')
    conhecidos = {v['id'] for v in vs}
    novos, feed_ids = [], []
    for e in entradas:  # feed vem do mais recente pro mais antigo
        vid = (re.search(r'<yt:videoId>([\w-]+)</yt:videoId>', e) or [None, None])[1]
        tit = (re.search(r'<title>([\s\S]*?)</title>', e) or [None, ''])[1]
        pub = (re.search(r'<published>([\d-]+)', e) or [None, ''])[1]
        if not vid: continue
        feed_ids.append(vid)
        if vid not in conhecidos:
            novos.append({'id': vid, 'titulo': _html.unescape(tit).strip(), 'views': '', 'data': pub})
        else:
            for v in vs:
                if v['id'] == vid and not v.get('data'): v['data'] = pub
    vs = novos + vs
    # espelha a recência do canal: quem está no feed sobe pro topo na ordem exata do YouTube
    pos = {vid: i for i, vid in enumerate(feed_ids)}
    vs.sort(key=lambda v: pos.get(v['id'], 10**6))  # estável: fora do feed mantém a ordem atual
    gravar(p('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    log('Coletor: %d vídeo(s) novo(s) no canal' % len(novos))
    return len(novos)

def transcrever(vid):
    """Legenda automática via innertube (client ANDROID). Levanta exceção se bloqueado/sem legenda."""
    s = requests.Session()
    s.headers['User-Agent'] = 'com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip'
    j = s.post('https://www.youtube.com/youtubei/v1/player', json={
        'context': {'client': {'clientName': 'ANDROID', 'clientVersion': '20.10.38',
                               'androidSdkVersion': 30, 'hl': 'pt', 'gl': 'BR'}},
        'videoId': vid}, timeout=60).json()
    tracks = (j.get('captions', {}).get('playerCaptionsTracklistRenderer', {}).get('captionTracks', []))
    tr = next((t for t in tracks if t.get('languageCode', '').startswith('pt')), tracks[0] if tracks else None)
    if not tr: raise RuntimeError('SEM_LEGENDA')
    dur = int(j.get('videoDetails', {}).get('lengthSeconds', 0) or 0)
    if 0 < dur < 240: raise RuntimeError('CURTO')  # shorts/teasers não entram no cérebro
    xml = s.get(tr['baseUrl'], timeout=60).text
    dec = lambda x: x.replace('&amp;','&').replace('&lt;','<').replace('&gt;','>').replace('&#39;',"'").replace('&quot;','"')
    parts = [dec(re.sub(r'<[^>]+>', ' ', m.group(1))) for m in re.finditer(r'<(?:text|p)[^>]*>([\s\S]*?)</(?:text|p)>', xml)]
    txt = re.sub(r'\s+', ' ', ' '.join(parts)).strip()
    if len(txt) < 2500: raise RuntimeError('CURTO')
    return txt

# ---------- 2 ANALISTA (probabilístico) ----------
SINTESE_SYS = """Você sintetiza vídeos do Alfredo Soares (co-fundador do G4 Educação) para uma base de conhecimento de negócios.
Responda APENAS com o markdown da síntese, em português, neste formato exato:

# {titulo}
url: https://www.youtube.com/watch?v={id}
views: {views}

## Resumo
(1-2 parágrafos densos)

## Conceitos e frameworks
- **Nome** — explicação (marque de quem é a lição se houver convidado)

## Insights acionáveis
- (5-12 itens práticos)

## Cases e números citados
- (empresas, pessoas, métricas)

## Frases marcantes
- "máx. 3 citações, cada uma com menos de 15 palavras"

## Temas
tags: (3-6 tags, OBRIGATORIAMENTE escolhidas desta lista: %s)""" % ', '.join(TAXONOMIA)

def sintetizar(v):
    txt = ler(p('transcricoes', v['id'] + '.txt'))
    md = llm(SINTESE_SYS, 'Vídeo: %s (id %s, %s views)\n\nTRANSCRIÇÃO:\n%s' % (v['titulo'], v['id'], v.get('views',''), txt[:180000]))
    gravar(p('sinteses', v['id'] + '.md'), md.strip())
    m = re.search(r'tags:\s*(.+)', md)
    return [t.strip() for t in m.group(1).split(',') if t.strip() in TAXONOMIA] if m else []

# ---------- 3 CONSOLIDADOR (probabilístico, incremental) ----------
CONSOL_SYS = """Você mantém a "mente do Alfredo Soares": arquivos de tópico com princípios numerados.
Receberá o arquivo atual do tópico e novas sínteses de vídeos. Reescreva o ARQUIVO COMPLETO do tópico:
- Mantenha o formato: título, princípios como **N. Título.** corpo com citação [Nome do vídeo · VIDEOID] e seção ## Fontes ao final.
- Funda repetições (o que se repete em vários vídeos ganha destaque como princípio central); divergências viram nuance; o vídeo mais recente prevalece em conflito.
- Não invente nada que não esteja nas fontes. Responda APENAS com o markdown do arquivo."""

def consolidar(tema, novos_ids):
    atual = ler(p('mente', tema + '.md'))
    sints = '\n\n---\n\n'.join(ler(p('sinteses', i + '.md')) for i in novos_ids)
    md = llm(CONSOL_SYS, 'TÓPICO: %s\n\nARQUIVO ATUAL:\n%s\n\nNOVAS SÍNTESES:\n%s' % (tema, atual or '(novo tópico)', sints), 12000)
    gravar(p('mente', tema + '.md'), md.strip())

# ---------- PIPELINE ----------
def processar(ids=None):
    if PROGRESSO['rodando']: return
    PROGRESSO['rodando'] = True; PROGRESSO['log'] = []
    try:
        if not ids:
            try: coletar()
            except Exception as e: log('Coletor falhou (segue com a fila atual): %s' % e)
        temas_novos = {}  # tema -> [video ids]
        for v in videos():
            if ids and v['id'] not in ids: continue
            try:
                if v['status'] == 'pendente':
                    log('Transcrevendo: ' + v['titulo'][:60])
                    try:
                        txt = transcrever(v['id'])
                    except RuntimeError as e:
                        if str(e) == 'CURTO': ignorar(v['id'], 'curto/short'); continue
                        raise
                    gravar(p('transcricoes', v['id'] + '.txt'), v['titulo'] + '\nhttps://www.youtube.com/watch?v=' + v['id'] + '\n' + txt)
                    v['status'] = 'transcrito'
                if v['status'] == 'transcrito':
                    log('Sintetizando: ' + v['titulo'][:60])
                    for t in sintetizar(v): temas_novos.setdefault(t, []).append(v['id'])
                    v['status'] = 'sintetizado'
                elif v['status'] == 'sintetizado':  # sintetizado mas nunca consolidado
                    md = ler(p('sinteses', v['id'] + '.md'))
                    m = re.search(r'tags:\s*(.+)', md)
                    for t in ([x.strip() for x in m.group(1).split(',')] if m else []):
                        if t in TAXONOMIA: temas_novos.setdefault(t, []).append(v['id'])
            except Exception as e:
                log('ERRO %s: %s' % (v['id'], e))
        for tema, ids in temas_novos.items():
            log('Consolidando tópico: ' + tema)
            try: consolidar(tema, ids)
            except Exception as e: log('ERRO consolidação %s: %s' % (tema, e)); continue
        cons = set(json.loads(ler(p('consolidado.json')) or '[]'))
        cons |= {i for ids in temas_novos.values() for i in ids}
        gravar(p('consolidado.json'), json.dumps(sorted(cons)))
        log('Pipeline concluído.')
    except Exception:
        log('ERRO geral: ' + traceback.format_exc()[-300:])
    finally:
        PROGRESSO['rodando'] = False

# ---------- CONTEXTO DA EMPRESA ----------
def extrair_texto(nome, dados):
    n = nome.lower()
    if n.endswith('.pdf'):
        from pypdf import PdfReader
        return '\n'.join(pg.extract_text() or '' for pg in PdfReader(io.BytesIO(dados)).pages)
    if n.endswith('.docx'):
        import docx
        d = docx.Document(io.BytesIO(dados))
        partes = [par.text for par in d.paragraphs]
        for t in d.tables:
            for row in t.rows: partes.append(' | '.join(c.text for c in row.cells))
        return '\n'.join(partes)
    if n.endswith(('.html', '.htm')):
        return re.sub(r'\s+', ' ', re.sub(r'<script[\s\S]*?</script>|<style[\s\S]*?</style>|<[^>]+>', ' ',
                                          dados.decode('utf-8', 'ignore')))
    return dados.decode('utf-8', 'ignore')

def contexto_empresa():
    partes = []
    perfil = ler(p('contexto.md'))
    if perfil: partes.append('PERFIL DA EMPRESA (escrito pelo dono):\n' + perfil)
    for f in sorted(glob.glob(p('contexto', '*.txt'))):
        partes.append('DOCUMENTO [%s]:\n%s' % (os.path.basename(f)[:-4], ler(f)[:30000]))
    return ('\n\n'.join(partes))[:120000]

# ---------- 4 PERSONA / CHAT ----------
def chat(pergunta, historico):
    persona = ler(p('mente', 'persona.md'))
    mente = '\n\n'.join(ler(f) for f in sorted(glob.glob(p('mente', '*.md'))) if not f.endswith('persona.md'))
    ctx = contexto_empresa()
    sys = persona + '\n\n=== BASE DE CONHECIMENTO (a mente) ===\n' + mente + \
          (('\n\n=== CONTEXTO DA EMPRESA DO USUÁRIO ===\n' + ctx) if ctx else '') + \
          '\n\nResponda como o advisor: direto, provocador, com plano de ação e a conta feita. Use o contexto da empresa quando existir — a orientação deve ser específica pro negócio do usuário. Cite os vídeos-fonte (nome + link) dos princípios que usar.'
    msgs = historico[-8:] + [{'role': 'user', 'content': pergunta}]
    r = requests.post(GW_URL + '/v1/messages',
        headers=_gw_headers(),
        json={'model': MODEL, 'max_tokens': 3000, 'system': sys, 'messages': msgs}, timeout=180)
    r.raise_for_status()
    return ''.join(b.get('text', '') for b in r.json().get('content', []))

# ---------- API ----------
@app.route('/api/estado')
def api_estado():
    topics = []
    for f in sorted(glob.glob(p('mente', '*.md'))):
        slug = os.path.basename(f)[:-3]
        if slug == 'persona': continue
        raw = ler(f)
        topics.append({'slug': slug, 'title': (re.search(r'^# (.+)$', raw, re.M) or [None,'?'])[1] if re.search(r'^# (.+)$', raw, re.M) else slug,
                       'n': len(re.findall(r'\*\*\d+\.', raw))})
    return jsonify({'videos': videos(), 'topics': topics, 'progresso': PROGRESSO,
                    'gateway': bool(GW_URL and GW_KEY)})

@app.route('/api/mente/<slug>')
def api_mente(slug):
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    return jsonify({'md': ler(p('mente', slug + '.md'))})

@app.route('/api/coletar', methods=['POST'])
def api_coletar():
    try:
        n = coletar()
        return jsonify({'ok': True, 'novos': n})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/processar', methods=['POST'])
def api_processar():
    ids = (request.get_json(silent=True) or {}).get('ids')
    threading.Thread(target=processar, args=(ids,), daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/ordem', methods=['POST'])
def api_ordem():
    ids = (request.get_json(force=True) or {}).get('ids', [])
    pos = {vid: i for i, vid in enumerate(ids)}
    vs = json.loads(ler(p('videos.json')) or '[]')
    vs.sort(key=lambda v: pos.get(v['id'], 10**6))  # sort estável: não listados mantêm ordem
    gravar(p('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    return jsonify({'ok': True})

@app.route('/api/contexto', methods=['GET', 'POST'])
def api_contexto():
    if request.method == 'POST':
        gravar(p('contexto.md'), request.get_json(force=True).get('perfil', ''))
        return jsonify({'ok': True})
    docs = [{'nome': os.path.basename(f)[:-4], 'chars': len(ler(f))} for f in sorted(glob.glob(p('contexto', '*.txt')))]
    return jsonify({'perfil': ler(p('contexto.md')), 'docs': docs})

@app.route('/api/contexto/upload', methods=['POST'])
def api_ctx_upload():
    f = request.files.get('arquivo')
    if not f: return jsonify({'erro': 'sem arquivo'}), 400
    dados = f.read()
    if len(dados) > 15 * 1024 * 1024: return jsonify({'erro': 'máx 15MB'}), 400
    try: txt = extrair_texto(f.filename, dados)
    except Exception as e: return jsonify({'erro': 'falha ao extrair: %s' % e}), 400
    nome = re.sub(r'[^\w.-]+', '_', os.path.splitext(f.filename)[0])[:60]
    gravar(p('contexto', nome + '.txt'), txt.strip())
    return jsonify({'ok': True, 'nome': nome, 'chars': len(txt)})

@app.route('/api/contexto/url', methods=['POST'])
def api_ctx_url():
    url = request.get_json(force=True).get('url', '').strip()
    if not url.startswith('http'): return jsonify({'erro': 'URL inválida'}), 400
    try:
        r = requests.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
        txt = extrair_texto('pagina.html', r.content)
    except Exception as e: return jsonify({'erro': 'falha ao buscar: %s' % e}), 400
    nome = 'site_' + re.sub(r'[^\w.-]+', '_', re.sub(r'^https?://', '', url))[:60]
    gravar(p('contexto', nome + '.txt'), (url + '\n' + txt).strip())
    return jsonify({'ok': True, 'nome': nome, 'chars': len(txt)})

@app.route('/api/contexto/doc/<nome>', methods=['DELETE'])
def api_ctx_del(nome):
    if not re.match(r'^[\w.-]+$', nome): return ('', 404)
    f = p('contexto', nome + '.txt')
    if os.path.exists(f): os.remove(f)
    return jsonify({'ok': True})

@app.route('/api/chat', methods=['POST'])
def api_chat():
    d = request.get_json(force=True)
    try:
        return jsonify({'resposta': chat(d.get('pergunta', ''), d.get('historico', []))})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/')
def painel():
    return Response(ler(os.path.join(BASE, 'painel.html')), mimetype='text/html')

# ---------- CRON ----------
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    def rotina_cron():
        # padrão: só coleta os vídeos novos (você decide o que processar).
        # CRON_AUTO=1 no Railway → processa a fila inteira automaticamente.
        if os.environ.get('CRON_AUTO') == '1': processar()
        else:
            try: coletar()
            except Exception as e: print('cron coletar:', e, flush=True)
    sched = BackgroundScheduler(timezone='America/Sao_Paulo')
    sched.add_job(rotina_cron, 'cron', hour=CRON_HORA, minute=0)
    sched.start()
except Exception as e:
    print('APScheduler não iniciado:', e)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)
