# -*- coding: utf-8 -*-
"""agente-advisor — Advisor Alfredo Soares (Sementes Maná)
Pipeline: coletar transcrição YouTube -> sintetizar (Claude via mana-llm-gateway) -> consolidar mente -> chat persona.
Estado derivado do filesystem (data/): transcricoes/{id}.txt, sinteses/{id}.md, mente/{tema}.md, consolidado.json
"""
import os, io, json, re, glob, threading, traceback, datetime
import requests
from flask import Flask, request, jsonify, Response

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get('DATA_DIR', os.path.join(BASE, 'data'))
_SEED = os.path.join(BASE, 'data')
# Volume persistente (Railway): se DATA_DIR aponta pra um volume vazio, semeia com o data/ do repo
if DATA != _SEED and not os.path.exists(os.path.join(DATA, 'advisors.json')) \
        and not os.path.exists(os.path.join(DATA, 'videos.json')) \
        and not os.path.isdir(os.path.join(DATA, 'advisors')):
    import shutil
    os.makedirs(DATA, exist_ok=True)
    shutil.copytree(_SEED, DATA, dirs_exist_ok=True)
    print('[boot] volume semeado a partir do repo', flush=True)

# ---------- MULTI-ADVISOR: dados do advisor ficam em data/advisors/<slug>/ ----------
ADVISOR_PADRAO = {'slug': 'alfredo-soares', 'nome': 'Alfredo Soares',
                  'descricao': 'Co-fundador do G4 Educação e da VTEX Brasil. Vendas, varejo e marca.',
                  'foto': '', 'canal': os.environ.get('YT_CHANNEL_ID', 'UCh9HMS4C3F02msM-kiilAdA'),
                  'ativo': True}
# chaves de dados que pertencem a UM advisor (o resto — ctx/, conversas.json — é compartilhado)
_ADV_DIRS = ('mente', 'sinteses', 'transcricoes')
_ADV_JSONS = ('videos.json', 'consolidado.json', 'ignorados.json')

def _migrar_multiadvisor():
    """Idempotente: se ainda está no layout antigo (dados na raiz), move p/ advisors/alfredo-soares/
    e cria advisors.json. Roda no boot; se advisors.json já existe, não faz nada."""
    import shutil
    reg = os.path.join(DATA, 'advisors.json')
    if os.path.exists(reg):
        return
    layout_antigo = os.path.isdir(os.path.join(DATA, 'mente')) or os.path.exists(os.path.join(DATA, 'videos.json'))
    destino = os.path.join(DATA, 'advisors', ADVISOR_PADRAO['slug'])
    os.makedirs(destino, exist_ok=True)
    if layout_antigo:
        for d in _ADV_DIRS:
            src = os.path.join(DATA, d)
            if os.path.isdir(src) and not os.path.isdir(os.path.join(destino, d)):
                shutil.move(src, os.path.join(destino, d))
        for jf in _ADV_JSONS:
            src = os.path.join(DATA, jf)
            if os.path.exists(src) and not os.path.exists(os.path.join(destino, jf)):
                shutil.move(src, os.path.join(destino, jf))
        print('[boot] migrado layout antigo -> advisors/%s/' % ADVISOR_PADRAO['slug'], flush=True)
    with open(reg, 'w', encoding='utf-8') as fh:
        json.dump([ADVISOR_PADRAO], fh, ensure_ascii=False, indent=1)
    print('[boot] advisors.json criado', flush=True)

_migrar_multiadvisor()
GW_URL = os.environ.get('LLM_GATEWAY_URL', '').rstrip('/')
GW_KEY = os.environ.get('LLM_GATEWAY_KEY', '')
MODEL = os.environ.get('LLM_MODEL', 'claude-sonnet-4-5')                 # chat do Advisor (qualidade)
MODEL_SINTESE = os.environ.get('LLM_MODEL_SINTESE', 'mana-rapido')       # síntese/consolidação (barato = Haiku)
CRON_HORA = int(os.environ.get('CRON_HORA', '7'))  # BRT
YT_CHANNEL_ID = os.environ.get('YT_CHANNEL_ID', 'UCh9HMS4C3F02msM-kiilAdA')  # @canaldoalfredosoares
PROXY_URL = os.environ.get('PROXY_URL', '')  # proxy residencial p/ YouTube (http://user:pass@host:porta)
PROXIES = {'http': PROXY_URL, 'https': PROXY_URL} if PROXY_URL else None
TAXONOMIA = ['modelo-de-negocio','vendas-e-ofertas','marketing-de-influencia','canais-e-varejo',
             'conteudo-e-audiencia','gestao-e-pessoas','mentalidade-empreendedora',
             'networking-e-conexoes','branding-e-posicionamento']

app = Flask(__name__)
PROGRESSO = {'rodando': False, 'log': [], 'abortar': False}

@app.before_request
def _preflight():
    if request.method == 'OPTIONS' and request.path.startswith('/api/'):
        r = Response('')
        r.headers['Access-Control-Allow-Origin'] = '*'
        r.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        r.headers['Access-Control-Allow-Headers'] = 'content-type'
        return r

@app.after_request
def _cors(resp):
    if request.path.startswith('/api/'):
        resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

def p(*a): return os.path.join(DATA, *a)          # raiz do volume (compartilhado: ctx/, conversas, advisors.json)
def ler(path): return open(path, encoding='utf-8').read() if os.path.exists(path) else ''
def gravar(path, txt):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, 'w', encoding='utf-8').write(txt)

# ---------- Registro de advisors + caminho por advisor ----------
def advisors():
    return json.loads(ler(p('advisors.json')) or '[]')

def salvar_advisors(lst):
    gravar(p('advisors.json'), json.dumps(lst, ensure_ascii=False, indent=1))

def advisor_ativo():
    a = advisors()
    return next((x for x in a if x.get('ativo')), a[0] if a else dict(ADVISOR_PADRAO))

def adv_slug():
    return advisor_ativo().get('slug', ADVISOR_PADRAO['slug'])

def pd(slug, *a):
    """Caminho dentro de UM advisor específico: data/advisors/<slug>/..."""
    return os.path.join(DATA, 'advisors', slug, *a)

def pa(*a):
    """Caminho DENTRO do advisor ativo: data/advisors/<slug>/..."""
    return pd(adv_slug(), *a)

def canal_do_advisor():
    return advisor_ativo().get('canal') or YT_CHANNEL_ID

def resolver_canal(canal):
    """Aceita id UC..., URL de canal ou @handle → devolve o channel_id (UC...) pro RSS."""
    canal = (canal or '').strip()
    m = re.search(r'(UC[\w-]{22})', canal)
    if m: return m.group(1)
    h = re.search(r'@([\w.\-]+)', canal)
    if h:
        try:
            r = requests.get('https://www.youtube.com/@' + h.group(1), timeout=30,
                             headers={'User-Agent': 'Mozilla/5.0'}, proxies=PROXIES)
            mm = re.search(r'"(?:channelId|externalId)":"(UC[\w-]{22})"', r.text)
            if mm: return mm.group(1)
        except Exception:
            pass
    return canal or YT_CHANNEL_ID

def advisor_por_canal(cid):
    """Casa o channel_id (UC...) do vídeo com o advisor cujo 'canal' tem o mesmo UC. Sem rede."""
    m = re.search(r'(UC[\w-]{22})', cid or '')
    if not m: return None
    cid = m.group(1)
    for a in advisors():
        cm = re.search(r'(UC[\w-]{22})', a.get('canal') or '')
        if cm and cm.group(1) == cid:
            return a['slug']
    return None

def rotear_advisor(advisor_in='', canal_in=''):
    """Pra qual advisor vai a transcrição: escolha explícita > match por canal > advisor ativo."""
    a = (advisor_in or '').strip()
    if a and any(x['slug'] == a for x in advisors()):
        return a, 'escolhido'
    alvo = advisor_por_canal(canal_in)
    if alvo:
        return alvo, 'canal'
    return adv_slug(), 'ativo'

def videos():
    vs = json.loads(ler(pa('videos.json')) or '[]')
    ign = set(json.loads(ler(pa('ignorados.json')) or '[]'))
    vs = [v for v in vs if v['id'] not in ign]
    cons = set(json.loads(ler(pa('consolidado.json')) or '[]'))
    for v in vs:
        if v['id'] in cons: v['status'] = 'consolidado'
        elif os.path.exists(pa('sinteses', v['id'] + '.md')): v['status'] = 'sintetizado'
        elif os.path.exists(pa('transcricoes', v['id'] + '.txt')): v['status'] = 'transcrito'
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

def llm(system, user, max_tokens=8000, model=None):
    r = requests.post(GW_URL + '/v1/messages',
        headers=_gw_headers(),
        json={'model': model or MODEL_SINTESE, 'max_tokens': max_tokens, 'system': system,
              'messages': [{'role': 'user', 'content': user}]}, timeout=300)
    r.raise_for_status()
    return ''.join(b.get('text', '') for b in r.json().get('content', []))

# ---------- 1 COLETOR (determinístico) ----------
import html as _html

def ignorar(vid, motivo):
    ign = json.loads(ler(pa('ignorados.json')) or '[]')
    if vid not in ign:
        ign.append(vid); gravar(pa('ignorados.json'), json.dumps(ign))
    log('Ignorado %s (%s)' % (vid, motivo))

def dur_seg(vid):
    """Duração do vídeo em segundos (0 se não conseguir determinar → deixa passar)."""
    try:
        s = requests.Session()
        if PROXIES: s.proxies = PROXIES
        s.headers['User-Agent'] = 'com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip'
        j = s.post('https://www.youtube.com/youtubei/v1/player', json={
            'context': {'client': {'clientName': 'ANDROID', 'clientVersion': '20.10.38',
                                   'androidSdkVersion': 30, 'hl': 'pt', 'gl': 'BR'}},
            'videoId': vid}, timeout=30).json()
        return int(j.get('videoDetails', {}).get('lengthSeconds', 0) or 0)
    except Exception:
        return 0

def coletar(cid=None):
    """Só RSS — rápido, traz todos os vídeos do canal DO ADVISOR ATIVO pra fila (limpeza de shorts é botão separado)."""
    cid = resolver_canal(cid or canal_do_advisor())
    r = requests.get('https://www.youtube.com/feeds/videos.xml?channel_id=' + cid,
                     timeout=30, headers={'User-Agent': 'Mozilla/5.0'}, proxies=PROXIES)
    r.raise_for_status()
    entradas = re.findall(r'<entry>([\s\S]*?)</entry>', r.text)
    vs = json.loads(ler(pa('videos.json')) or '[]')
    conhecidos = {v['id'] for v in vs}
    novos, feed_ids = [], []
    for e in entradas:  # feed vem do mais recente pro mais antigo
        vid = (re.search(r'<yt:videoId>([\w-]+)</yt:videoId>', e) or [None, None])[1]
        tit = (re.search(r'<title>([\s\S]*?)</title>', e) or [None, ''])[1]
        pub = (re.search(r'<published>([\d-]+)', e) or [None, ''])[1]
        if not vid: continue
        feed_ids.append(vid)
        if vid not in conhecidos:
            item = {'id': vid, 'titulo': _html.unescape(tit).strip(), 'views': '', 'data': pub}
            d = dur_seg(vid)              # traz a duração já no Buscar (só p/ os novos = poucos)
            if d > 0: item['dur'] = d
            novos.append(item)
        else:
            for v in vs:
                if v['id'] == vid and not v.get('data'): v['data'] = pub
    vs = novos + vs
    pos = {vid: i for i, vid in enumerate(feed_ids)}
    vs.sort(key=lambda v: pos.get(v['id'], 10**6))
    gravar(pa('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    log('Coletor: %d vídeo(s) novo(s) no canal' % len(novos))
    return len(novos)

def limpar_shorts():
    """REGRA LOCAL pura — remove da fila quem já tem duração < 4min. Não fala com o YouTube, é instantâneo."""
    PROGRESSO['log'] = []
    vs = json.loads(ler(pa('videos.json')) or '[]')
    rem = [v['id'] for v in vs if 0 < (v.get('dur') or 0) < 240]
    for vid in rem: ignorar(vid, 'short (regra local)')
    vs2 = [v for v in vs if v['id'] not in set(rem)]
    gravar(pa('videos.json'), json.dumps(vs2, ensure_ascii=False, indent=1))
    sem = sum(1 for v in vs2 if not v.get('dur'))
    log('🧹 %d short(s) removido(s) pela duração.' % len(rem) +
        ((' %d vídeo(s) ainda sem duração — clique em Buscar (traz a duração dos novos).' % sem) if sem else ''))

def transcrever(vid):
    """3 tentativas com pausa — o YouTube bloqueia IP de datacenter de forma intermitente."""
    import time
    ult = None
    for _ in range(3):
        try:
            return _transcrever_1x(vid)
        except RuntimeError as e:
            if str(e) == 'CURTO': raise
            ult = e; time.sleep(10)
    raise ult

def _transcrever_1x(vid):
    """Legenda automática via innertube (client ANDROID). Levanta exceção se bloqueado/sem legenda."""
    s = requests.Session()
    if PROXIES: s.proxies = PROXIES
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
SINTESE_SYS = """Você sintetiza um vídeo de negócios para uma base de conhecimento. Se o vídeo for uma mesa/entrevista com várias pessoas, ATRIBUA cada conceito/insight a quem o defendeu (coloque o nome entre parênteses ao lado do ponto).
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

def sintetizar(v, slug=None):
    slug = slug or adv_slug()
    txt = ler(pd(slug, 'transcricoes', v['id'] + '.txt'))
    md = llm(SINTESE_SYS, 'Vídeo: %s (id %s, %s views)\n\nTRANSCRIÇÃO:\n%s' % (v['titulo'], v['id'], v.get('views',''), txt[:180000]))
    gravar(pd(slug, 'sinteses', v['id'] + '.md'), md.strip())
    m = re.search(r'tags:\s*(.+)', md)
    return [t.strip() for t in m.group(1).split(',') if t.strip() in TAXONOMIA] if m else []

# ---------- 3 CONSOLIDADOR (roteia cada princípio p/ UM tema-dono; filtra por locutor) ----------
ROTA_SYS = """Você transforma a síntese de UM vídeo em princípios para a "mente" que TREINA o Advisor **%(nome)s** (um conselheiro de negócios), roteando cada princípio para UM ÚNICO tema (o mais central).

ATRIBUIÇÃO POR LOCUTOR (importante): o vídeo pode ser uma MESA/ENTREVISTA com várias pessoas. Extraia para a mente de %(nome)s SOMENTE os princípios que o PRÓPRIO %(nome)s defendeu ou disse. Ignore o que outra pessoa nomeada (que não seja %(nome)s) claramente defendeu. Se for uma fala SOLO de %(nome)s — ou não dá pra distinguir quem falou — aproveite todos os princípios fortes. Trate variações de grafia do nome como a mesma pessoa (ex.: Dener/Denner, Lipert/Lippert, Lázaro/Lasaro).

Temas válidos (use exatamente estes slugs): %(temas)s
Regras:
- Cada princípio vai para UM tema só — o mais específico. NUNCA coloque a mesma ideia em temas diferentes.
- Gere de 4 a 10 princípios NO TOTAL (não por tema), cobrindo o que %(nome)s tem de mais forte. Se %(nome)s falou pouco, gere menos — só o que é dele.
- Título curto (3-6 palavras).
- Corpo RICO, em UMA única linha (NÃO quebre linha dentro do princípio), com 2 a 4 frases trazendo: (1) o princípio, (2) o MECANISMO / como aplicar na prática, e (3) os números, nomes, cases e valores concretos citados. Escreva como orientação de consultor: específico e acionável, nunca genérico.
- Não invente; use só o que está na síntese.
SAÍDA: uma linha por princípio, no formato EXATO abaixo (sem markdown, sem numeração, sem nada além das linhas):
tema-slug ||| Título curto ||| corpo do princípio"""

def rotear(v, nome=None, slug=None):
    """1 chamada de LLM: extrai princípios da síntese (só do locutor 'nome'), cada um com seu tema-dono."""
    slug = slug or adv_slug()
    nome = nome or next((a['nome'] for a in advisors() if a['slug'] == slug), slug)
    sint = ler(pd(slug, 'sinteses', v['id'] + '.md'))
    out = llm(ROTA_SYS % {'nome': nome, 'temas': ', '.join(TAXONOMIA)}, 'SÍNTESE:\n' + sint[:20000], 4500)
    princ = []
    for ln in out.splitlines():
        parts = [x.strip() for x in ln.split('|||')]
        if len(parts) >= 3 and parts[0] in TAXONOMIA and parts[1] and parts[2]:
            princ.append((parts[0], parts[1], parts[2]))
    return princ

def arquivar(v, princ, slug=None):
    """Grava cada princípio no seu tema-dono do advisor (append, numeração contínua, fonte única)."""
    slug = slug or adv_slug()
    tit = v.get('titulo', v['id']); vid = v['id']
    porTema = {}
    for tema, t, c in princ:
        porTema.setdefault(tema, []).append((t, c))
    for tema, itens in porTema.items():
        atual = ler(pd(slug, 'mente', tema + '.md'))
        prox = len(re.findall(r'\*\*\d+\.', atual)) + 1
        m = re.search(r'\n##\s*Fontes\b[\s\S]*$', atual)
        if m:            corpo, fontes = atual[:m.start()].rstrip(), atual[m.start():].strip()
        elif atual.strip(): corpo, fontes = atual.rstrip(), '## Fontes'
        else:            corpo, fontes = '# ' + tema, '## Fontes'
        blocos = []
        for t, c in itens:
            blocos.append('**%d. %s.** %s [%s · %s]' % (prox, t.strip().rstrip('.'), c, tit, vid))
            prox += 1
        corpo += '\n\n' + '\n\n'.join(blocos)
        if vid not in fontes:
            fontes = fontes.rstrip() + '\n- %s · %s' % (tit, vid)
        gravar(pd(slug, 'mente', tema + '.md'), (corpo + '\n\n' + fontes).strip() + '\n')

def remover_video_da_mente(slug, vid):
    """Tira da mente do advisor todos os princípios citados daquele vídeo; renumera e conserta Fontes."""
    for f in glob.glob(pd(slug, 'mente', '*.md')):
        if f.endswith('persona.md'): continue
        raw = ler(f)
        mf = re.search(r'\n##\s*Fontes\b[\s\S]*$', raw)
        corpo, fontes = (raw[:mf.start()], raw[mf.start():]) if mf else (raw, '')
        blocos = re.split(r'(?=\*\*\d+\.)', corpo)
        cabecalho = blocos[0].rstrip()
        mantidos = [b.strip() for b in blocos[1:] if ('· ' + vid + ']') not in b]
        renum = []
        for i, b in enumerate(mantidos, 1):
            renum.append(re.sub(r'^\*\*\d+\.', '**%d.' % i, b))
        if fontes:
            linhas = [ln for ln in fontes.splitlines() if ('· ' + vid) not in ln]
            fontes = '\n'.join(linhas).strip()
        novo = cabecalho
        if renum: novo += '\n\n' + '\n\n'.join(renum)
        if fontes: novo += '\n\n' + fontes
        gravar(f, novo.strip() + '\n')

def reconsolidar_video(target_slug, vid):
    """Recorta a mente do advisor p/ este vídeo: remove o que estava e re-roteia SÓ os princípios dele (filtro por locutor)."""
    nome = next((a['nome'] for a in advisors() if a['slug'] == target_slug), target_slug)
    v = next((x for x in json.loads(ler(pd(target_slug, 'videos.json')) or '[]') if x['id'] == vid), {'id': vid, 'titulo': vid})
    if not os.path.exists(pd(target_slug, 'sinteses', vid + '.md')):
        if not ler(pd(target_slug, 'transcricoes', vid + '.txt')):
            return {'erro': 'sem transcrição desse vídeo no advisor'}
        sintetizar(v, target_slug)
    remover_video_da_mente(target_slug, vid)
    princ = rotear(v, nome, target_slug)
    if princ: arquivar(v, princ, target_slug)
    cons = set(json.loads(ler(pd(target_slug, 'consolidado.json')) or '[]')) | {vid}
    gravar(pd(target_slug, 'consolidado.json'), json.dumps(sorted(cons)))
    return {'ok': True, 'principios': len(princ)}

# ---------- PIPELINE ----------
def processar(ids=None):
    if PROGRESSO['rodando']: return
    PROGRESSO['rodando'] = True; PROGRESSO['log'] = []; PROGRESSO['abortar'] = False
    try:
        if not ids:
            try: coletar()
            except Exception as e: log('Coletor falhou (segue com a fila atual): %s' % e)
        a_rotear = []   # vídeos prontos p/ rotear na mente (cada princípio → 1 tema-dono)
        for v in videos():
            if PROGRESSO['abortar']:
                log('⛔ Abortado pelo usuário — o que já foi sintetizado entra na próxima rodada.'); break
            if ids and v['id'] not in ids: continue
            try:
                if v['status'] == 'pendente':
                    log('Transcrevendo: ' + v['titulo'][:60])
                    try:
                        txt = transcrever(v['id'])
                    except RuntimeError as e:
                        if str(e) == 'CURTO': ignorar(v['id'], 'curto/short'); continue
                        raise
                    gravar(pa('transcricoes', v['id'] + '.txt'), v['titulo'] + '\nhttps://www.youtube.com/watch?v=' + v['id'] + '\n' + txt)
                    v['status'] = 'transcrito'
                if v['status'] == 'transcrito':
                    log('Sintetizando: ' + v['titulo'][:60])
                    sintetizar(v)
                    v['status'] = 'sintetizado'
                    a_rotear.append(v)
                elif v['status'] == 'sintetizado':   # sintetizado mas ainda não consolidado
                    a_rotear.append(v)
            except Exception as e:
                log('ERRO %s: %s' % (v['id'], e))
        if PROGRESSO['abortar']: a_rotear = []
        consolidados = []
        for v in a_rotear:
            if PROGRESSO['abortar']: break
            log('Consolidando na mente: ' + v['titulo'][:50])
            try:
                princ = rotear(v)
                if princ: arquivar(v, princ)
                consolidados.append(v['id'])
            except Exception as e:
                log('ERRO consolidação %s: %s' % (v['id'], e)); continue
        cons = set(json.loads(ler(pa('consolidado.json')) or '[]')) | set(consolidados)
        gravar(pa('consolidado.json'), json.dumps(sorted(cons)))
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

# ---------- CONTEXTO: Empresa -> Área -> itens ----------
def _slug(s):
    s = re.sub(r'[^\w\s-]', '', (s or '').strip().lower())
    return re.sub(r'[\s_-]+', '-', s)[:50] or 'sem-nome'

CTX = 'ctx'  # data/ctx/

def empresas():
    return json.loads(ler(p(CTX, 'empresas.json')) or '[]')

def salvar_empresas(lst):
    gravar(p(CTX, 'empresas.json'), json.dumps(lst, ensure_ascii=False, indent=1))

def garantir_empresa_padrao():
    if not empresas():
        salvar_empresas([{'slug': 'sementes-mana', 'nome': 'Sementes Maná', 'ativa': True}])
        gravar(p(CTX, 'sementes-mana', 'perfil.md'), '')

def empresa_ativa():
    es = empresas()
    return next((e for e in es if e.get('ativa')), es[0] if es else None)

def ctx_arvore(slug):
    """Retorna {perfil, areas:[{slug,nome,nota,docs:[{nome,chars}]}]} de uma empresa."""
    base = p(CTX, slug)
    perfil = ler(os.path.join(base, 'perfil.md'))
    areas = []
    if os.path.isdir(base):
        for a in sorted(os.listdir(base)):
            ad = os.path.join(base, a)
            if not os.path.isdir(ad): continue
            docs = [{'nome': os.path.basename(f)[:-4], 'chars': len(ler(f))}
                    for f in sorted(glob.glob(os.path.join(ad, '*.txt')))
                    if not os.path.basename(f).startswith('_')]
            nome = ler(os.path.join(ad, '_nome.txt')) or a
            areas.append({'slug': a, 'nome': nome.strip() or a, 'nota': ler(os.path.join(ad, '_nota.md')), 'docs': docs})
    return {'perfil': perfil, 'areas': areas}

def _empresa_por_slug(slug):
    es = empresas()
    return next((x for x in es if x['slug'] == slug), None) or empresa_ativa()

def contexto_para_chat(empresa_slug=None, area_slug=None):
    """Monta o contexto de uma empresa (default: ativa). Se area_slug, foca só naquela área."""
    e = _empresa_por_slug(empresa_slug)
    if not e: return ''
    arv = ctx_arvore(e['slug'])
    partes = ['EMPRESA: ' + e['nome']]
    if arv['perfil'].strip(): partes.append('PERFIL GERAL:\n' + arv['perfil'].strip())
    for a in arv['areas']:
        if area_slug and a['slug'] != area_slug: continue
        bloco = ['--- ÁREA: %s ---' % a['nome']]
        if a['nota'].strip(): bloco.append(a['nota'].strip())
        for f in sorted(glob.glob(p(CTX, e['slug'], a['slug'], '*.txt'))):
            if os.path.basename(f).startswith('_'): continue
            bloco.append('[doc: %s]\n%s' % (os.path.basename(f)[:-4], ler(f)[:25000]))
        if len(bloco) > 1: partes.append('\n'.join(bloco))
    return ('\n\n'.join(partes))[:150000]

# ---------- 4 PERSONA / CHAT ----------
def chat(pergunta, historico, empresa_slug=None, area_slug=None, advisor_slug=None):
    aslug = advisor_slug if (advisor_slug and any(x['slug'] == advisor_slug for x in advisors())) else adv_slug()
    persona = ler(pd(aslug, 'mente', 'persona.md'))
    mente = '\n\n'.join(ler(f) for f in sorted(glob.glob(pd(aslug, 'mente', '*.md'))) if not f.endswith('persona.md'))
    ctx = contexto_para_chat(empresa_slug, area_slug)
    sys = persona + '\n\n=== BASE DE CONHECIMENTO (a mente) ===\n' + mente + \
          (('\n\n=== CONTEXTO DA EMPRESA (use e cite a área de origem quando relevante) ===\n' + ctx) if ctx else '') + \
          '\n\nResponda como o advisor: direto, provocador, com plano de ação e a conta feita. Use o contexto da empresa quando existir — a orientação deve ser específica pro negócio. Cite os vídeos-fonte (nome + link) dos princípios que usar.'
    msgs = historico[-8:] + [{'role': 'user', 'content': pergunta}]
    r = requests.post(GW_URL + '/v1/messages',
        headers=_gw_headers(),
        json={'model': MODEL, 'max_tokens': 3000, 'system': sys, 'messages': msgs}, timeout=180)
    r.raise_for_status()
    return ''.join(b.get('text', '') for b in r.json().get('content', []))

# ---------- MESA REDONDA: vários advisors opinam + moderador sintetiza ----------
MESA_SYS = """Você é o MODERADOR de uma mesa redonda de conselheiros de negócios. Recebeu o parecer INDEPENDENTE de cada advisor sobre a MESMA pergunta do empresário.
Entregue UM conselho unificado da mesa, em português, nesta estrutura EXATA (use os títulos):
**Consenso** — os pontos em que os conselheiros concordam.
**Divergências** — onde discordam e por quê (diga qual advisor defende cada lado).
**Plano de ação da mesa** — os passos concretos recomendados, com a conta feita quando houver números.
Regras: não invente; use só o que os pareceres trazem. Seja direto e específico; sintetize, não repita cada parecer por extenso."""

def mesa(pergunta, historico, slugs, empresa_slug=None, area_slug=None):
    """Cada advisor responde pela mente dele (reusa chat); depois o moderador costura o conselho da mesa."""
    validos = [s for s in slugs if any(x['slug'] == s for x in advisors())]
    pareceres = []
    for s in validos:
        nome = next((a['nome'] for a in advisors() if a['slug'] == s), s)
        try:
            resp = chat(pergunta, historico, empresa_slug, area_slug, s)
        except Exception as e:
            resp = '(não consegui o parecer: %s)' % e
        pareceres.append({'slug': s, 'nome': nome, 'resposta': resp})
    corpo = '\n\n'.join('### Parecer de %s\n%s' % (pp['nome'], pp['resposta']) for pp in pareceres)
    sintese = llm(MESA_SYS, 'PERGUNTA DO EMPRESÁRIO:\n%s\n\nPARECERES DA MESA:\n%s' % (pergunta, corpo), 3000, MODEL)
    return pareceres, sintese

# ---------- API ----------
@app.route('/api/estado')
def api_estado():
    topics = []
    for f in sorted(glob.glob(pa('mente', '*.md'))):
        slug = os.path.basename(f)[:-3]
        if slug == 'persona': continue
        raw = ler(f)
        topics.append({'slug': slug, 'title': (re.search(r'^# (.+)$', raw, re.M) or [None,'?'])[1] if re.search(r'^# (.+)$', raw, re.M) else slug,
                       'n': len(re.findall(r'\*\*\d+\.', raw))})
    return jsonify({'videos': videos(), 'topics': topics, 'progresso': PROGRESSO,
                    'gateway': bool(GW_URL and GW_KEY),
                    'advisors': advisors(), 'advisor': advisor_ativo()})

@app.route('/api/mente/<slug>')
def api_mente(slug):
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    return jsonify({'md': ler(pa('mente', slug + '.md'))})

@app.route('/api/transcricao', methods=['POST', 'OPTIONS'])
def api_transcricao():
    """Plano B: recebe transcrição extraída no navegador do usuário (quando o YouTube bloqueia o IP do servidor)."""
    resp_headers = {'Access-Control-Allow-Origin': 'https://www.youtube.com',
                    'Access-Control-Allow-Methods': 'POST',
                    'Access-Control-Allow-Headers': 'content-type'}
    if request.method == 'OPTIONS':
        return Response('', headers=resp_headers)
    d = request.get_json(force=True)
    vid, txt = d.get('id', ''), (d.get('texto', '') or '').strip()
    titulo_in = (d.get('titulo') or '').strip()
    if not re.match(r'^[\w-]{11}$', vid): return jsonify({'erro': 'id inválido'}), 400
    if len(txt) < 2500:
        # NÃO ignora mais (era footgun: glitch de carregamento no navegador travava o vídeo)
        return jsonify({'ok': True, 'curto': True, 'chars': len(txt)}), 200, resp_headers
    # roteia: advisor escolhido na extensão > canal do vídeo bate com um advisor > advisor ativo
    alvo, motivo = rotear_advisor(d.get('advisor', ''), d.get('canal', ''))
    nome_alvo = next((a['nome'] for a in advisors() if a['slug'] == alvo), alvo)
    vlist = json.loads(ler(pd(alvo, 'videos.json')) or '[]')
    vs = {v['id']: v for v in vlist}
    novo = vid not in vs
    if novo:                                 # vídeo de QUALQUER canal → entra na fila automaticamente
        vs[vid] = {'id': vid, 'titulo': titulo_in or vid, 'views': '', 'data': '', 'fonte': 'extensão'}
        vlist = [vs[vid]] + vlist
        gravar(pd(alvo, 'videos.json'), json.dumps(vlist, ensure_ascii=False, indent=1))
    elif titulo_in and vs[vid].get('titulo') in (None, '', vid):   # completa título vazio
        vs[vid]['titulo'] = titulo_in
        gravar(pd(alvo, 'videos.json'), json.dumps(vlist, ensure_ascii=False, indent=1))
    tit = vs[vid].get('titulo', vid)
    gravar(pd(alvo, 'transcricoes', vid + '.txt'), tit + '\nhttps://www.youtube.com/watch?v=' + vid + '\n' + txt)
    ign = json.loads(ler(pd(alvo, 'ignorados.json')) or '[]')   # chegou transcrição válida → reativa se estava ignorado por engano
    if vid in ign:
        gravar(pd(alvo, 'ignorados.json'), json.dumps([x for x in ign if x != vid]))
    return jsonify({'ok': True, 'chars': len(txt), 'novo': novo,
                    'advisor': alvo, 'advisor_nome': nome_alvo, 'roteamento': motivo}), 200, resp_headers

@app.route('/api/limpar-shorts', methods=['POST'])
def api_limpar_shorts():
    limpar_shorts()  # regra local, instantâneo
    return jsonify({'ok': True})

@app.route('/api/set-duracoes', methods=['POST'])
def api_set_duracoes():
    """Recebe {id: segundos} extraídos pelo navegador (IP residencial) e grava a duração na fila."""
    m = (request.get_json(force=True) or {}).get('duracoes', {})
    vs = json.loads(ler(pa('videos.json')) or '[]')
    n = 0
    for v in vs:
        if v['id'] in m:
            v['dur'] = int(m[v['id']]); n += 1
    gravar(pa('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    return jsonify({'ok': True, 'atualizados': n})

@app.route('/api/set-meta', methods=['POST'])
def api_set_meta():
    """Corrige título/duração/fonte de vídeos (ex.: os que ficaram sem nome ao adicionar sob bloqueio)."""
    m = (request.get_json(force=True) or {}).get('metas', {})  # {id: {titulo, dur, fonte}}
    vs = json.loads(ler(pa('videos.json')) or '[]')
    n = 0
    for v in vs:
        if v['id'] in m:
            mm = m[v['id']]
            if mm.get('titulo'): v['titulo'] = mm['titulo']
            if mm.get('dur'): v['dur'] = int(mm['dur'])
            if mm.get('fonte'): v['fonte'] = mm['fonte']
            n += 1
    gravar(pa('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    return jsonify({'ok': True, 'atualizados': n})

@app.route('/api/sem-titulo')
def api_sem_titulo():
    """Lista vídeos cujo título ficou igual ao id (pro navegador re-buscar os nomes)."""
    vs = json.loads(ler(pa('videos.json')) or '[]')
    return jsonify([v['id'] for v in vs if v.get('titulo', '') == v['id']])

# ---------- Síntese feita no Cowork (subagentes) — app só grava, sem LLM ----------
@app.route('/api/transcricao/<vid>')
def api_get_transcricao(vid):
    if not re.match(r'^[\w-]{11}$', vid): return ('', 404)
    txt = ler(pa('transcricoes', vid + '.txt'))
    if not txt: return jsonify({}), 404
    tit = next((v['titulo'] for v in json.loads(ler(pa('videos.json')) or '[]') if v['id'] == vid), vid)
    return jsonify({'id': vid, 'titulo': tit, 'texto': txt})

@app.route('/api/pendentes-sintese')
def api_pendentes_sintese():
    """IDs que têm transcrição mas ainda não têm síntese."""
    out = []
    for f in sorted(glob.glob(pa('transcricoes', '*.txt'))):
        vid = os.path.basename(f)[:-4]
        if not os.path.exists(pa('sinteses', vid + '.md')):
            tit = next((v['titulo'] for v in json.loads(ler(pa('videos.json')) or '[]') if v['id'] == vid), vid)
            out.append({'id': vid, 'titulo': tit})
    return jsonify(out)

@app.route('/api/set-sintese/<vid>', methods=['POST'])
def api_set_sintese(vid):
    """Recebe a síntese .md já pronta (feita no Cowork) e grava — sem chamar LLM."""
    if not re.match(r'^[\w-]{11}$', vid): return ('', 404)
    gravar(pa('sinteses', vid + '.md'), (request.get_json(force=True) or {}).get('md', '').strip())
    return jsonify({'ok': True})

@app.route('/api/importar-lote', methods=['POST'])
def api_importar_lote():
    """Recebe TUDO de uma vez (sínteses + mente + ids consolidados), feito no Cowork. Grava sem LLM."""
    d = request.get_json(force=True) or {}
    n_s = n_m = 0
    for vid, md in (d.get('sinteses', {}) or {}).items():
        if re.match(r'^[\w-]{11}$', vid): gravar(pa('sinteses', vid + '.md'), (md or '').strip()); n_s += 1
    for tema, md in (d.get('mente', {}) or {}).items():
        if re.match(r'^[\w-]+$', tema): gravar(pa('mente', tema + '.md'), (md or '').strip()); n_m += 1
    ids = d.get('consolidado', [])
    if ids:
        cons = set(json.loads(ler(pa('consolidado.json')) or '[]')) | set(ids)
        gravar(pa('consolidado.json'), json.dumps(sorted(cons)))
    return jsonify({'ok': True, 'sinteses': n_s, 'temas': n_m})

@app.route('/api/set-mente/<tema>', methods=['POST'])
def api_set_mente(tema):
    """Recebe o arquivo .md de um tópico da mente (consolidado no Cowork) e grava; marca ids como consolidados."""
    if not re.match(r'^[\w-]+$', tema): return ('', 404)
    d = request.get_json(force=True) or {}
    gravar(pa('mente', tema + '.md'), (d.get('md', '') or '').strip())
    ids = d.get('ids', [])
    if ids:
        cons = set(json.loads(ler(pa('consolidado.json')) or '[]')) | set(ids)
        gravar(pa('consolidado.json'), json.dumps(sorted(cons)))
    return jsonify({'ok': True})

@app.route('/api/ignorar', methods=['POST'])
def api_ignorar():
    """Remove uma lista de IDs da fila e manda pros ignorados (usado p/ limpar shorts identificados fora)."""
    ids = set((request.get_json(force=True) or {}).get('ids', []))
    vs = json.loads(ler(pa('videos.json')) or '[]')
    manter = [v for v in vs if v['id'] not in ids]
    gravar(pa('videos.json'), json.dumps(manter, ensure_ascii=False, indent=1))
    for vid in ids: ignorar(vid, 'short (lote)')
    return jsonify({'ok': True, 'removidos': len(vs) - len(manter)})

@app.route('/api/abortar', methods=['POST'])
def api_abortar():
    PROGRESSO['abortar'] = True
    return jsonify({'ok': True})

def yt_meta(vid):
    import time
    for tent in range(3):
        try:
            s = requests.Session()
            if PROXIES: s.proxies = PROXIES
            s.headers['User-Agent'] = 'com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip'
            j = s.post('https://www.youtube.com/youtubei/v1/player', json={
                'context': {'client': {'clientName': 'ANDROID', 'clientVersion': '20.10.38',
                                       'androidSdkVersion': 30, 'hl': 'pt', 'gl': 'BR'}},
                'videoId': vid}, timeout=30).json()
            d = j.get('videoDetails', {})
            if d.get('title'):
                return {'titulo': d['title'], 'dur': int(d.get('lengthSeconds', 0) or 0), 'autor': d.get('author', '')}
        except Exception:
            pass
        if tent < 2: time.sleep(4)
    return {'titulo': vid, 'dur': 0, 'autor': ''}

def extrair_video_id(tok):
    tok = tok.strip()
    m = re.search(r'(?:v=|youtu\.be/|shorts/|embed/|/live/)([\w-]{11})', tok)
    if m: return m.group(1)
    return tok if re.match(r'^[\w-]{11}$', tok) else None

@app.route('/api/add-video', methods=['POST'])
def api_add_video():
    txt = (request.get_json(force=True) or {}).get('urls', '')
    ids, vistos = [], set()
    for tok in re.split(r'[\s,]+', txt.strip()):
        vid = extrair_video_id(tok)
        if vid and vid not in vistos: ids.append(vid); vistos.add(vid)
    if not ids: return jsonify({'erro': 'nenhuma URL válida'}), 400
    vs = json.loads(ler(pa('videos.json')) or '[]')
    conhecidos = {v['id'] for v in vs}
    add, novos = 0, []
    for vid in ids:
        if vid in conhecidos: continue
        try: m = yt_meta(vid)
        except Exception: m = {'titulo': vid, 'dur': 0, 'autor': ''}
        item = {'id': vid, 'titulo': m['titulo'], 'views': '', 'data': '', 'fonte': m.get('autor') or 'avulso'}
        if m['dur'] > 0: item['dur'] = m['dur']
        novos.append(item); conhecidos.add(vid); add += 1
    vs = novos + vs
    gravar(pa('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    return jsonify({'ok': True, 'adicionados': add, 'total_validas': len(ids)})

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

@app.route('/api/reconsolidar', methods=['POST'])
def api_reconsolidar():
    """Recorta a mente de UM advisor p/ UM vídeo (filtro por locutor). Síncrono: usa LLM, pode levar alguns segundos."""
    d = request.get_json(force=True) or {}
    slug, vid = d.get('advisor', ''), d.get('id', '')
    if not any(a['slug'] == slug for a in advisors()):
        return jsonify({'erro': 'advisor não encontrado'}), 400
    if not re.match(r'^[\w-]{11}$', vid):
        return jsonify({'erro': 'id inválido'}), 400
    try:
        return jsonify(reconsolidar_video(slug, vid))
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/ordem', methods=['POST'])
def api_ordem():
    ids = (request.get_json(force=True) or {}).get('ids', [])
    pos = {vid: i for i, vid in enumerate(ids)}
    vs = json.loads(ler(pa('videos.json')) or '[]')
    vs.sort(key=lambda v: pos.get(v['id'], 10**6))  # sort estável: não listados mantêm ordem
    gravar(pa('videos.json'), json.dumps(vs, ensure_ascii=False, indent=1))
    return jsonify({'ok': True})

@app.route('/api/contexto')
def api_contexto():
    garantir_empresa_padrao()
    e = empresa_ativa()
    return jsonify({'empresas': empresas(), 'ativa': e['slug'] if e else None,
                    'arvore': ctx_arvore(e['slug']) if e else {'perfil': '', 'areas': []}})

@app.route('/api/contexto/empresa', methods=['POST'])
def api_ctx_empresa_nova():
    nome = (request.get_json(force=True) or {}).get('nome', '').strip()
    if not nome: return jsonify({'erro': 'informe o nome'}), 400
    es = empresas(); slug = _slug(nome)
    if any(x['slug'] == slug for x in es): return jsonify({'erro': 'empresa já existe'}), 400
    for x in es: x['ativa'] = False
    es.append({'slug': slug, 'nome': nome, 'ativa': True})
    salvar_empresas(es); gravar(p(CTX, slug, 'perfil.md'), '')
    return jsonify({'ok': True, 'slug': slug})

@app.route('/api/contexto/empresa-ativa', methods=['POST'])
def api_ctx_empresa_ativa():
    slug = (request.get_json(force=True) or {}).get('slug', '')
    es = empresas()
    if not any(x['slug'] == slug for x in es): return jsonify({'erro': 'não achei'}), 404
    for x in es: x['ativa'] = (x['slug'] == slug)
    salvar_empresas(es); return jsonify({'ok': True})

@app.route('/api/contexto/empresa/<slug>', methods=['DELETE'])
def api_ctx_empresa_del(slug):
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    es = [x for x in empresas() if x['slug'] != slug]
    if es and not any(x.get('ativa') for x in es): es[0]['ativa'] = True
    salvar_empresas(es)
    import shutil
    if os.path.isdir(p(CTX, slug)): shutil.rmtree(p(CTX, slug))
    return jsonify({'ok': True})

@app.route('/api/contexto/perfil', methods=['POST'])
def api_ctx_perfil():
    d = request.get_json(force=True) or {}
    slug = d.get('empresa', '')
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    gravar(p(CTX, slug, 'perfil.md'), d.get('perfil', ''))
    return jsonify({'ok': True})

@app.route('/api/contexto/area', methods=['POST'])
def api_ctx_area_nova():
    d = request.get_json(force=True) or {}
    emp, nome = d.get('empresa', ''), (d.get('nome', '') or '').strip()
    if not re.match(r'^[\w-]+$', emp) or not nome: return jsonify({'erro': 'dados'}), 400
    aslug = _slug(nome)
    gravar(p(CTX, emp, aslug, '_nome.txt'), nome)
    if not os.path.exists(p(CTX, emp, aslug, '_nota.md')): gravar(p(CTX, emp, aslug, '_nota.md'), '')
    return jsonify({'ok': True, 'slug': aslug})

@app.route('/api/contexto/area/<emp>/<area>', methods=['DELETE'])
def api_ctx_area_del(emp, area):
    if not re.match(r'^[\w-]+$', emp) or not re.match(r'^[\w-]+$', area): return ('', 404)
    import shutil
    if os.path.isdir(p(CTX, emp, area)): shutil.rmtree(p(CTX, emp, area))
    return jsonify({'ok': True})

@app.route('/api/contexto/nota', methods=['POST'])
def api_ctx_nota():
    d = request.get_json(force=True) or {}
    emp, area = d.get('empresa', ''), d.get('area', '')
    if not re.match(r'^[\w-]+$', emp) or not re.match(r'^[\w-]+$', area): return ('', 404)
    gravar(p(CTX, emp, area, '_nota.md'), d.get('nota', ''))
    return jsonify({'ok': True})

@app.route('/api/contexto/upload', methods=['POST'])
def api_ctx_upload():
    emp, area = request.form.get('empresa', ''), request.form.get('area', '')
    if not re.match(r'^[\w-]+$', emp) or not re.match(r'^[\w-]+$', area):
        return jsonify({'erro': 'escolha empresa e área'}), 400
    f = request.files.get('arquivo')
    if not f: return jsonify({'erro': 'sem arquivo'}), 400
    dados = f.read()
    if len(dados) > 15 * 1024 * 1024: return jsonify({'erro': 'máx 15MB'}), 400
    try: txt = extrair_texto(f.filename, dados)
    except Exception as e: return jsonify({'erro': 'falha ao extrair: %s' % e}), 400
    nome = re.sub(r'[^\w.-]+', '_', os.path.splitext(f.filename)[0])[:60]
    gravar(p(CTX, emp, area, nome + '.txt'), txt.strip())
    return jsonify({'ok': True, 'nome': nome, 'chars': len(txt)})

@app.route('/api/contexto/url', methods=['POST'])
def api_ctx_url():
    d = request.get_json(force=True) or {}
    emp, area, url = d.get('empresa', ''), d.get('area', ''), (d.get('url', '') or '').strip()
    if not re.match(r'^[\w-]+$', emp) or not re.match(r'^[\w-]+$', area):
        return jsonify({'erro': 'escolha empresa e área'}), 400
    if not url.startswith('http'): return jsonify({'erro': 'URL inválida'}), 400
    try:
        r = requests.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
        txt = extrair_texto('pagina.html', r.content)
    except Exception as e: return jsonify({'erro': 'falha ao buscar: %s' % e}), 400
    nome = 'site_' + re.sub(r'[^\w.-]+', '_', re.sub(r'^https?://', '', url))[:50]
    gravar(p(CTX, emp, area, nome + '.txt'), (url + '\n' + txt).strip())
    return jsonify({'ok': True, 'nome': nome, 'chars': len(txt)})

@app.route('/api/contexto/doc/<emp>/<area>/<nome>', methods=['DELETE'])
def api_ctx_del(emp, area, nome):
    if not all(re.match(r'^[\w.-]+$', x) for x in (emp, area, nome)): return ('', 404)
    f = p(CTX, emp, area, nome + '.txt')
    if os.path.exists(f): os.remove(f)
    return jsonify({'ok': True})

def registrar_conversa(pergunta, resposta, emp_nome='', emp_slug='', area=''):
    conv = json.loads(ler(p('conversas.json')) or '[]')
    item = {'id': max([c['id'] for c in conv], default=0) + 1,
            'data': datetime.datetime.now().isoformat(timespec='seconds'),
            'empresa': emp_nome, 'empresa_slug': emp_slug, 'area': area,
            'pergunta': pergunta, 'resposta': resposta}
    conv.append(item)
    gravar(p('conversas.json'), json.dumps(conv, ensure_ascii=False))
    return item

@app.route('/api/chat', methods=['POST'])
def api_chat():
    d = request.get_json(force=True)
    emp_slug, area = d.get('empresa') or None, d.get('area') or None
    adv = d.get('advisor') or None
    try:
        resp = chat(d.get('pergunta', ''), d.get('historico', []), emp_slug, area, adv)
        e = _empresa_por_slug(emp_slug)
        item = registrar_conversa(d.get('pergunta', ''), resp, e['nome'] if e else '', e['slug'] if e else '', area or '')
        return jsonify({'resposta': resp, 'id': item['id'], 'data': item['data'], 'empresa': item['empresa'], 'area': area or ''})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/mesa', methods=['POST'])
def api_mesa():
    d = request.get_json(force=True)
    slugs = d.get('advisors') or []
    if len([s for s in slugs if any(x['slug'] == s for x in advisors())]) < 2:
        return jsonify({'erro': 'escolha pelo menos 2 advisors pra mesa'}), 400
    emp_slug, area = d.get('empresa') or None, d.get('area') or None
    try:
        pareceres, sintese = mesa(d.get('pergunta', ''), d.get('historico', []), slugs, emp_slug, area)
        e = _empresa_por_slug(emp_slug)
        nomes = ', '.join(pp['nome'] for pp in pareceres)
        item = registrar_conversa(d.get('pergunta', ''), sintese, e['nome'] if e else '',
                                  e['slug'] if e else '', 'Mesa: ' + nomes)
        return jsonify({'pareceres': pareceres, 'sintese': sintese,
                        'id': item['id'], 'data': item['data'], 'empresa': item['empresa']})
    except Exception as e:
        return jsonify({'erro': str(e)}), 500

@app.route('/api/empresas-areas')
def api_empresas_areas():
    garantir_empresa_padrao()
    out = []
    for e in empresas():
        arv = ctx_arvore(e['slug'])
        out.append({'slug': e['slug'], 'nome': e['nome'], 'ativa': e.get('ativa'),
                    'areas': [{'slug': a['slug'], 'nome': a['nome']} for a in arv['areas']]})
    return jsonify(out)

@app.route('/api/conversas')
def api_conversas():
    q = request.args.get('q', '').lower().strip()
    emp = request.args.get('empresa', '').strip()
    conv = sorted(json.loads(ler(p('conversas.json')) or '[]'), key=lambda c: c['id'], reverse=True)
    if emp:
        conv = [c for c in conv if c.get('empresa_slug', '') == emp]
    if q:
        conv = [c for c in conv if q in (c['pergunta'] + ' ' + c['resposta'] + ' ' + c.get('empresa', '')).lower()]
    return jsonify([{'id': c['id'], 'data': c['data'], 'empresa': c.get('empresa', ''), 'area': c.get('area', ''),
                     'pergunta': c['pergunta'], 'preview': c['resposta'][:180]} for c in conv])

@app.route('/api/conversa/<int:cid>')
def api_conversa(cid):
    conv = json.loads(ler(p('conversas.json')) or '[]')
    c = next((x for x in conv if x['id'] == cid), None)
    return (jsonify(c), 200) if c else (jsonify({}), 404)

@app.route('/api/conversa/<int:cid>', methods=['DELETE'])
def api_conversa_del(cid):
    conv = [x for x in json.loads(ler(p('conversas.json')) or '[]') if x['id'] != cid]
    gravar(p('conversas.json'), json.dumps(conv, ensure_ascii=False))
    return jsonify({'ok': True})

@app.route('/api/conversas', methods=['DELETE'])
def api_conversas_limpar():
    emp = request.args.get('empresa', '').strip()
    conv = json.loads(ler(p('conversas.json')) or '[]')
    if emp:
        conv = [c for c in conv if c.get('empresa_slug', '') != emp]
    else:
        conv = []
    gravar(p('conversas.json'), json.dumps(conv, ensure_ascii=False))
    return jsonify({'ok': True})

# ---------- ADVISORS (múltiplas personalidades) ----------
PERSONA_SEED = """# Persona — %(nome)s

Você é **%(nome)s**. %(descricao)s

Fale em primeira pessoa, direto e prático. Dê o plano de ação e a conta feita.
Use SEMPRE a base de conhecimento (a mente) abaixo e cite os vídeos-fonte dos princípios que usar.
Não invente números nem cases: use só o que está na mente e no contexto da empresa.
"""

def criar_advisor_dados(slug, nome, descricao):
    """Cria a pasta do advisor com persona inicial e jsons vazios (não sobrescreve se já existir)."""
    base = os.path.join(DATA, 'advisors', slug)
    if not os.path.exists(os.path.join(base, 'mente', 'persona.md')):
        gravar(os.path.join(base, 'mente', 'persona.md'),
               PERSONA_SEED % {'nome': nome, 'descricao': descricao or ''})
    for jf in _ADV_JSONS:
        fp = os.path.join(base, jf)
        if not os.path.exists(fp):
            gravar(fp, '[]')

@app.route('/api/advisors')
def api_advisors():
    return jsonify({'advisors': advisors(), 'ativo': adv_slug()})

@app.route('/api/advisor-ativo', methods=['POST'])
def api_advisor_ativo():
    slug = (request.get_json(force=True) or {}).get('slug', '')
    lst = advisors()
    if not any(x['slug'] == slug for x in lst):
        return jsonify({'erro': 'advisor não encontrado'}), 404
    for x in lst: x['ativo'] = (x['slug'] == slug)
    salvar_advisors(lst)
    return jsonify({'ok': True, 'ativo': slug})

@app.route('/api/advisor', methods=['POST'])
def api_advisor_novo():
    d = request.get_json(force=True) or {}
    nome = (d.get('nome', '') or '').strip()
    if not nome: return jsonify({'erro': 'informe o nome'}), 400
    slug = _slug(nome)
    lst = advisors()
    if any(x['slug'] == slug for x in lst):
        return jsonify({'erro': 'já existe um advisor com esse nome'}), 400
    desc = (d.get('descricao', '') or '').strip()
    canal = (d.get('canal', '') or '').strip()
    criar_advisor_dados(slug, nome, desc)
    for x in lst: x['ativo'] = False
    lst.append({'slug': slug, 'nome': nome, 'descricao': desc, 'foto': '', 'canal': canal, 'ativo': True})
    salvar_advisors(lst)
    return jsonify({'ok': True, 'slug': slug})

@app.route('/api/advisor/<slug>', methods=['POST'])
def api_advisor_editar(slug):
    """Edita nome/descrição de um advisor existente (não mexe nos dados/mente)."""
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    d = request.get_json(force=True) or {}
    lst = advisors()
    x = next((a for a in lst if a['slug'] == slug), None)
    if not x: return jsonify({'erro': 'não encontrado'}), 404
    if d.get('nome'): x['nome'] = d['nome'].strip()
    if 'descricao' in d: x['descricao'] = (d.get('descricao') or '').strip()
    if 'canal' in d: x['canal'] = (d.get('canal') or '').strip()
    salvar_advisors(lst)
    return jsonify({'ok': True})

@app.route('/api/advisor/<slug>', methods=['DELETE'])
def api_advisor_del(slug):
    """Exclui um advisor do registro. Os dados NÃO são apagados: a pasta é renomeada
    p/ _deleted-<slug> (recuperável). Não deixa excluir o único advisor."""
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    lst = advisors()
    if len(lst) <= 1: return jsonify({'erro': 'não dá pra excluir o único advisor'}), 400
    if not any(a['slug'] == slug for a in lst): return jsonify({'erro': 'não encontrado'}), 404
    era_ativo = any(a['slug'] == slug and a.get('ativo') for a in lst)
    lst = [a for a in lst if a['slug'] != slug]
    if era_ativo and lst: lst[0]['ativo'] = True
    salvar_advisors(lst)
    import shutil
    src = os.path.join(DATA, 'advisors', slug)
    if os.path.isdir(src):
        dest = os.path.join(DATA, 'advisors', '_deleted-' + slug)
        i = 1
        while os.path.exists(dest):
            dest = os.path.join(DATA, 'advisors', '_deleted-%s-%d' % (slug, i)); i += 1
        shutil.move(src, dest)
    return jsonify({'ok': True, 'ativo': (lst[0]['slug'] if lst else None)})

@app.route('/api/advisor-foto/<slug>', methods=['POST'])
def api_advisor_foto_up(slug):
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    lst = advisors()
    x = next((a for a in lst if a['slug'] == slug), None)
    if not x: return jsonify({'erro': 'não encontrado'}), 404
    f = request.files.get('arquivo')
    if not f: return jsonify({'erro': 'sem arquivo'}), 400
    dados = f.read()
    if len(dados) > 5 * 1024 * 1024: return jsonify({'erro': 'máx 5MB'}), 400
    ext = (os.path.splitext(f.filename)[1].lower() or '.jpg').lstrip('.')
    if ext not in ('jpg', 'jpeg', 'png', 'webp', 'gif'): return jsonify({'erro': 'imagem inválida'}), 400
    fn = 'foto.' + ext
    caminho = os.path.join(DATA, 'advisors', slug, fn)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, 'wb') as fh: fh.write(dados)
    x['foto'] = fn
    salvar_advisors(lst)
    return jsonify({'ok': True, 'foto': fn})

@app.route('/api/advisor-foto/<slug>')
def api_advisor_foto(slug):
    if not re.match(r'^[\w-]+$', slug): return ('', 404)
    x = next((a for a in advisors() if a['slug'] == slug), None)
    fn = (x or {}).get('foto', '')
    if not fn: return ('', 404)
    caminho = os.path.join(DATA, 'advisors', slug, fn)
    if not os.path.exists(caminho): return ('', 404)
    mimes = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'webp': 'image/webp', 'gif': 'image/gif'}
    mime = mimes.get(fn.rsplit('.', 1)[-1].lower(), 'application/octet-stream')
    with open(caminho, 'rb') as fh: dados = fh.read()
    return Response(dados, mimetype=mime, headers={'Cache-Control': 'no-cache'})

@app.route('/api/backup')
def api_backup():
    """Zip de TODO o data/ (mente, sínteses, transcrições, jsons) — rede de segurança do volume."""
    import io as _io, zipfile, datetime as _dt
    buf = _io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(DATA):
            for f in files:
                full = os.path.join(root, f)
                try: z.write(full, os.path.relpath(full, DATA))
                except Exception: pass
    fn = 'advisor-backup-%s.zip' % _dt.datetime.now().strftime('%Y%m%d')
    return Response(buf.getvalue(), mimetype='application/zip',
                    headers={'Content-Disposition': 'attachment; filename="%s"' % fn})

# ============================================================================
#  PLAYBOOK POR SEGMENTO — doutrina do advisor + geração de HTML personalizado
#  Bloco ADITIVO: não altera nenhuma função existente do app.py.
#  Inserir em app.py logo ANTES de:  @app.route('/')  /  def painel()
# ============================================================================

DOUTRINA_ARQ = 'doutrina.md'
PLAYBOOKS_DIR = 'playbooks'

def doutrina(slug=None):
    """Doutrina consolidada do advisor: data/advisors/<slug>/doutrina.md.
    Fallback: a mente inteira (menos persona), se ainda não subiram a doutrina."""
    s = slug or adv_slug()
    d = ler(pd(s, DOUTRINA_ARQ))
    if d.strip():
        return d
    return '\n\n'.join(ler(f) for f in sorted(glob.glob(pd(s, 'mente', '*.md')))
                       if not f.endswith('persona.md'))

PLAYBOOK_SYS = """Você é %(nome)s. Não é um assistente resumindo o %(nome)s: você É ele, com o repertório
dele, respondendo a um empresário que te contratou como advisor.

Você recebe (1) a sua DOUTRINA — os princípios extraídos das suas próprias falas, cada um com o módulo
(Mx.x) e o videoId de origem — e (2) o CONTEXTO de um negócio real. Entregue o PLAYBOOK desse negócio.

=== AS 5 REGRAS INVIOLÁVEIS ===
1. CITE A FONTE. Toda recomendação carrega o módulo e o videoId de onde ela vem, assim:
   <span class="fonte">M4.1 · bXYnTmjo5AU</span>
   Recomendação sem fonte na doutrina NÃO ENTRA no playbook. Nunca invente videoId.
2. TRADUZA, NÃO TRANSPLANTE. O princípio é seu; o exemplo tem que ser DO SEGMENTO do cliente.
   Nunca mande o cara "fazer o que a Growth Supplements fez" — mostre o que o princípio da Growth
   vira DENTRO do negócio dele, com o vocabulário e a realidade dele.
3. ONDE A DOUTRINA FOR FINA, DIGA QUE É FINA. Se o acervo não cobre bem aquele segmento ou aquele
   ponto, escreva isso numa caixa <div class="lacuna"> e diga o que falta. Não preencha buraco com
   invenção nem com lugar-comum de internet.
4. NUNCA RECOMENDE SEM A CONTA. Se você tem os números, faça a conta na frente dele. Se não tem,
   PERGUNTE o número numa caixa <div class="pergunta"> — não chute. "Matemática não é ideia."
5. RESPEITE A ORDEM DA ESPINHA. Modelo de negócio antes de aquisição; aquisição antes de venda.
   Não se resolve conversão de um modelo de negócio errado.

=== O PROTOCOLO (a ordem do raciocínio) ===
Bloco 0 — ARQUITETURA: que negócio é esse (negócio ou empresa?); como o segmento cobra hoje e como
deveria cobrar; qual é a conta que ninguém nesse segmento faz.
Bloco 1 — MARCA: o posicionamento que sustenta o preço; quem é a audiência certa.
Bloco 2 — AUDIÊNCIA E CANAL: a máquina de conteúdo viável para ESSE tipo de dono (não para um
influenciador full-time).
Bloco 3 — DEMANDA: os 3 motores de aquisição desse segmento; o evento/ritual que ele pode operar;
quem forma opinião nesse nicho.
Bloco 4 — CONVERSÃO: como o lead é qualificado, distribuído e fechado; a oferta; a remuneração do time.
Bloco 5 — EXPANSÃO: as outras prateleiras do MESMO cliente; o que gera recompra e recorrência aqui.
Bloco 6 — CANAIS DE TERCEIROS: quem já tem o cliente que ele quer, e se a oferta vale 20-30%% para esse
parceiro (senão ele não bota energia).
FECHO — os 5 próximos passos, cada um com NOME, NÚMERO e PRAZO. Nunca "invista em marketing".

=== TOM ===
Direto, provocador, matemático, generoso. Você separa o que a pessoa confunde. Você dá o diagnóstico
duro em uma frase. Você faz a conta antes de dar a ideia. Português do Brasil.

=== FORMATO DE SAÍDA ===
Devolva SOMENTE o conteúdo HTML do corpo — nada de <html>, <head>, <body>, <style> ou ```html.
Comece direto em <section>. Use apenas estas tags/classes (o CSS já existe):
<section>...</section>                          bloco principal
<h2>Bloco 0 — Arquitetura do negócio</h2>       título de bloco
<h3>...</h3>                                    subtítulo
<p>...</p>  <ul><li>...</li></ul>  <b>  <table><tr><th><td>
<blockquote>fala sua, verbatim, tirada da doutrina</blockquote>
<div class="conta">a conta feita, passo a passo</div>
<div class="acao">ação concreta com nome, número e prazo</div>
<div class="pergunta">número que falta e que ele precisa te dar</div>
<div class="lacuna">onde a doutrina não cobre bem esse segmento</div>
<span class="fonte">Mx.x · videoId</span>
Entregue o playbook COMPLETO, todos os blocos. Sem preâmbulo e sem despedida."""

PLAYBOOK_CSS = """*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0e12;color:#d6d8e0;font:15px/1.75 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:0}
.wrap{max-width:860px;margin:0 auto;padding:48px 20px 90px}
.cab{border-bottom:1px solid #2a2d38;padding-bottom:26px;margin-bottom:38px}
.eyebrow{color:#8b8e98;font-size:12px;letter-spacing:.13em;text-transform:uppercase;margin-bottom:10px}
h1{color:#e8c07a;font-size:clamp(24px,5vw,34px);line-height:1.25;font-weight:800;letter-spacing:-.02em}
.sub{color:#8b8e98;font-size:14px;margin-top:12px}
h2{color:#e8c07a;font-size:clamp(19px,4vw,23px);margin:52px 0 6px;font-weight:800;letter-spacing:-.01em}
h2::after{content:'';display:block;width:46px;height:2px;background:#6a5426;margin-top:12px}
h3{color:#c9cbd4;font-size:16.5px;margin:30px 0 10px;font-weight:700}
p{margin:12px 0}
ul{margin:12px 0 12px 20px}li{margin:7px 0}
b{color:#f0f1f5}
blockquote{border-left:3px solid #6a5426;background:#14161c;margin:20px 0;padding:15px 20px;border-radius:0 9px 9px 0;color:#c9cbd4;font-style:italic}
table{width:100%;border-collapse:collapse;margin:20px 0;font-size:13.5px;display:block;overflow-x:auto}
th{background:#181a21;color:#e8c07a;text-align:left;font-weight:700}
th,td{border:1px solid #2a2d38;padding:10px 13px;vertical-align:top}
.conta,.acao,.pergunta,.lacuna{margin:20px 0;padding:15px 19px;border-radius:11px;font-size:14.2px}
.conta{background:#11161b;border:1px solid #2b4150}
.conta::before{content:'A CONTA';display:block;color:#6fa8c7;font-size:11px;letter-spacing:.13em;font-weight:800;margin-bottom:8px}
.acao{background:#101a12;border:1px solid #2c4a32}
.acao::before{content:'FAZER';display:block;color:#71b37f;font-size:11px;letter-spacing:.13em;font-weight:800;margin-bottom:8px}
.pergunta{background:#1a1710;border:1px solid #524126}
.pergunta::before{content:'ME DIZ ESSE NÚMERO';display:block;color:#d1a55c;font-size:11px;letter-spacing:.13em;font-weight:800;margin-bottom:8px}
.lacuna{background:#1a1214;border:1px solid #5a2f38}
.lacuna::before{content:'AQUI A DOUTRINA É FINA';display:block;color:#d1737f;font-size:11px;letter-spacing:.13em;font-weight:800;margin-bottom:8px}
.fonte{display:inline-block;background:#181a21;border:1px solid #2a2d38;color:#8b8e98;font-size:11px;
  font-family:ui-monospace,SFMono-Regular,Menlo,monospace;padding:2px 8px;border-radius:6px;margin-left:5px;white-space:nowrap}
.fonte.suspeita{border-color:#7a3a44;color:#d1737f}
.fonte.suspeita::after{content:" ?"}
.rodape{margin-top:70px;border-top:1px solid #2a2d38;padding-top:22px;color:#6c6f7a;font-size:12px;line-height:1.7}
@media(max-width:600px){.wrap{padding:30px 16px 70px}}"""

PLAYBOOK_DOC = """<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>%(titulo)s</title><style>%(css)s</style></head><body><div class="wrap">
<div class="cab"><div class="eyebrow">Playbook · %(nome)s</div><h1>%(titulo)s</h1>
<div class="sub">%(sub)s</div></div>
%(corpo)s
<div class="rodape">Gerado pelo agente-advisor a partir da doutrina de %(nome)s (%(chars)s caracteres,
%(nvideos)s vídeos-fonte). Cada recomendação cita o módulo e o videoId de origem.<br>%(carimbo)s</div>
</div></body></html>"""

def _nvideos(slug):
    try:
        return len(json.loads(ler(pd(slug, 'consolidado.json')) or '[]'))
    except Exception:
        return 0

def _ids_reais(slug):
    """Os videoIds que realmente existem na base do advisor (consolidados + transcritos)."""
    ids = set()
    # 1) a DOUTRINA e a fonte de verdade: e o texto que o modelo leu pra citar
    ids |= set(re.findall(r'`([A-Za-z0-9_-]{11})`', doutrina(slug)))
    # 2) reforca com o que existe de fato na base do advisor
    try:
        ids |= set(json.loads(ler(pd(slug, 'consolidado.json')) or '[]'))
    except Exception:
        pass
    ids |= {os.path.basename(f)[:-4] for f in glob.glob(pd(slug, 'transcricoes', '*.txt'))}
    return {i for i in ids if len(i) == 11}

def _auditar_fontes(html, validos):
    """O LLM as vezes corrompe 1 caractere do videoId (enL6... -> enLG...), o que
    quebra o link e cria rastreabilidade falsa. Corrige quando ha UM unico candidato
    a distancia 1; marca como suspeita quando nao da pra ter certeza."""
    corrigidos, suspeitos = [], []
    if not validos:
        return html, corrigidos, suspeitos

    def troca(m):
        miolo = m.group(2)
        achado = re.search(r'[A-Za-z0-9_-]{11}', miolo)
        if not achado:
            return m.group(0)
        vid = achado.group(0)
        if vid in validos:
            return m.group(0)
        cand = [v for v in validos if sum(a != b for a, b in zip(v, vid)) == 1]
        if len(cand) == 1:
            corrigidos.append('%s->%s' % (vid, cand[0]))
            return m.group(1) + miolo.replace(vid, cand[0]) + m.group(3)
        suspeitos.append(vid)
        return '<span class="fonte suspeita">' + miolo + m.group(3)

    html = re.sub(r'(<span class="fonte">)(.*?)(</span>)', troca, html, flags=re.S)
    return html, corrigidos, suspeitos

def montar_playbook(segmento, empresa_slug=None, area_slug=None, advisor_slug=None,
                    perguntas='', titulo=''):
    """Gera o playbook HTML: doutrina do advisor + contexto da empresa + segmento."""
    aslug = advisor_slug if (advisor_slug and any(x['slug'] == advisor_slug for x in advisors())) else adv_slug()
    adv = next((x for x in advisors() if x['slug'] == aslug), dict(ADVISOR_PADRAO))
    dout = doutrina(aslug)
    if not dout.strip():
        raise RuntimeError('Advisor "%s" está sem doutrina e sem mente.' % aslug)

    ctx = contexto_para_chat(empresa_slug, area_slug)
    emp = _empresa_por_slug(empresa_slug)
    emp_nome = (emp or {}).get('nome', '')

    user = ['=== DOUTRINA DE %s ===' % adv['nome'].upper(), dout,
            '', '=== O NEGÓCIO ===', 'SEGMENTO: ' + segmento]
    if ctx.strip():
        user += ['', '=== CONTEXTO DA EMPRESA (do banco de contexto) ===', ctx]
    else:
        user += ['', '(Sem contexto cadastrado: trabalhe com o segmento e PERGUNTE os números que faltarem.)']
    if perguntas.strip():
        user += ['', '=== O QUE ELE QUER RESOLVER ===', perguntas.strip()]
    user += ['', 'Entregue o playbook completo agora, em HTML, seguindo o protocolo e as 5 regras.']

    corpo = llm(PLAYBOOK_SYS % {'nome': adv['nome']}, '\n'.join(user),
                max_tokens=16000, model=MODEL)
    corpo = re.sub(r'^\s*```(?:html)?\s*|\s*```\s*$', '', corpo.strip())
    corpo, corrigidos, suspeitos = _auditar_fontes(corpo, _ids_reais(aslug))
    if corrigidos:
        log('playbook: videoId corrigido -> %s' % ', '.join(corrigidos))
    if suspeitos:
        log('playbook: videoId SUSPEITO (marcado no HTML) -> %s' % ', '.join(suspeitos))

    tit = titulo.strip() or ('Playbook — ' + (emp_nome or segmento))
    sub = ' · '.join(x for x in [emp_nome, segmento] if x)
    doc = PLAYBOOK_DOC % {'titulo': tit, 'css': PLAYBOOK_CSS, 'corpo': corpo, 'sub': sub,
                          'nome': adv['nome'], 'chars': '{:,}'.format(len(dout)).replace(',', '.'),
                          'nvideos': _nvideos(aslug),
                          'carimbo': datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}
    return doc, aslug, tit, corrigidos, suspeitos

def _pb_nome(titulo):
    return '%s-%s.html' % (datetime.datetime.now().strftime('%Y%m%d-%H%M'), _slug(titulo))

def _semear_doutrina():
    """Se o volume ja estava semeado, o copytree do boot nao traz doutrina.md nova.
    Este seed e idempotente: so copia do repo pro volume quando ainda nao existe la."""
    if DATA == _SEED:
        return
    for a in advisors():
        slug = a.get('slug', '')
        origem = os.path.join(_SEED, 'advisors', slug, DOUTRINA_ARQ)
        alvo = pd(slug, DOUTRINA_ARQ)
        if os.path.exists(origem) and not os.path.exists(alvo):
            gravar(alvo, ler(origem))
            print('[boot] doutrina semeada: %s (%d chars)' % (slug, len(ler(alvo))), flush=True)

_semear_doutrina()

@app.route('/api/doutrina', methods=['GET', 'POST'])
def api_doutrina():
    """GET  /api/doutrina?advisor=slug   -> {slug, chars, texto}
       POST {advisor, texto}             -> grava data/advisors/<slug>/doutrina.md"""
    if request.method == 'GET':
        s = request.args.get('advisor') or adv_slug()
        return jsonify({'slug': s, 'chars': len(ler(pd(s, DOUTRINA_ARQ))), 'texto': ler(pd(s, DOUTRINA_ARQ))})
    d = request.get_json(force=True) or {}
    s = d.get('advisor') or adv_slug()
    txt = d.get('texto', '')
    if not txt.strip():
        return jsonify({'erro': 'texto vazio'}), 400
    gravar(pd(s, DOUTRINA_ARQ), txt)
    log('doutrina gravada: %s (%d chars)' % (s, len(txt)))
    return jsonify({'ok': True, 'slug': s, 'chars': len(txt)})

@app.route('/api/playbook', methods=['POST'])
def api_playbook():
    """POST {segmento, empresa?, area?, advisor?, perguntas?, titulo?, salvar?}
       -> {ok, arquivo, url, chars, html}"""
    d = request.get_json(force=True) or {}
    seg = (d.get('segmento') or '').strip()
    if not seg:
        return jsonify({'erro': 'informe o segmento'}), 400
    try:
        html, aslug, tit, corrigidos, suspeitos = montar_playbook(
            seg, d.get('empresa'), d.get('area'), d.get('advisor'),
            d.get('perguntas', ''), d.get('titulo', ''))
    except Exception as e:
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500
    arq = ''
    if d.get('salvar', True):
        arq = _pb_nome(tit)
        gravar(pd(aslug, PLAYBOOKS_DIR, arq), html)
        log('playbook gerado: %s' % arq)
    return jsonify({'ok': True, 'arquivo': arq, 'url': ('/api/playbook/' + arq) if arq else '',
                    'chars': len(html), 'html': html,
                    'fontes_corrigidas': corrigidos, 'fontes_suspeitas': suspeitos})

@app.route('/api/playbooks')
def api_playbooks():
    s = request.args.get('advisor') or adv_slug()
    out = []
    for f in sorted(glob.glob(pd(s, PLAYBOOKS_DIR, '*.html')), reverse=True):
        n = os.path.basename(f)
        out.append({'arquivo': n, 'titulo': n[14:-5].replace('-', ' '),
                    'quando': n[:13], 'chars': os.path.getsize(f)})
    return jsonify(out)

@app.route('/api/playbook/<arq>')
def api_playbook_get(arq):
    s = request.args.get('advisor') or adv_slug()
    if '/' in arq or '\\' in arq or not arq.endswith('.html'):
        return jsonify({'erro': 'nome inválido'}), 400
    h = ler(pd(s, PLAYBOOKS_DIR, arq))
    if not h:
        return jsonify({'erro': 'não encontrado'}), 404
    return Response(h, mimetype='text/html')

@app.route('/api/playbook/<arq>', methods=['DELETE'])
def api_playbook_del(arq):
    s = request.args.get('advisor') or adv_slug()
    if '/' in arq or '\\' in arq or not arq.endswith('.html'):
        return jsonify({'erro': 'nome inválido'}), 400
    f = pd(s, PLAYBOOKS_DIR, arq)
    if os.path.exists(f):
        os.remove(f)
    return jsonify({'ok': True})

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
