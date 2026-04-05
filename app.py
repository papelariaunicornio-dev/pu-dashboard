"""
Dashboard Analytics — Papelaria Unicórnio
FastAPI backend que consulta diretamente o PostgreSQL analytics.
"""
import os
from contextlib import contextmanager
from datetime import date, timedelta, datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pgpool
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="PU Dashboard")

PG = dict(
    host=os.getenv("PG_HOST", "postgres-analytics-kk4wgo8s8gk0wckswok4oc4o"),
    port=int(os.getenv("PG_PORT", 5432)),
    user=os.getenv("PG_USER", "analytics"),
    password=os.getenv("PG_PASS", "analytics_pass_pu2024"),
    dbname=os.getenv("PG_DB", "analytics"),
)

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = pgpool.SimpleConnectionPool(1, 10, **PG)
    return _pool

@contextmanager
def db():
    conn = get_pool().getconn()
    try:
        yield conn
    finally:
        get_pool().putconn(conn)

def q(sql: str, params=None) -> list:
    with db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]

def scalar(sql: str, params=None):
    rows = q(sql, params)
    if rows:
        return list(rows[0].values())[0]
    return None

# Converte coluna texto para DATE no PostgreSQL, suportando DD/MM/YYYY e YYYY-MM-DD
def dc(col: str) -> str:
    return f"""CASE
        WHEN {col} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN ({col})::date
        WHEN {col} ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}' THEN
            (SUBSTR({col},7,4)||'-'||SUBSTR({col},4,2)||'-'||SUBSTR({col},1,2))::date
        ELSE NULL END"""

def defaults(data_ini, data_fim):
    if not data_ini:
        data_ini = date.today().replace(day=1).isoformat()
    if not data_fim:
        data_fim = date.today().isoformat()
    return data_ini, data_fim

# ─────────────────────────────────────────────
# SERVE HTML
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def root():
    with open("/app/static/index.html", encoding="utf-8") as f:
        return f.read()

# ─────────────────────────────────────────────
# GERAL
# ─────────────────────────────────────────────
@app.get("/api/geral/kpis")
def geral_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    vendas = q(f"""
        SELECT
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS faturamento,
            COALESCE(AVG(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS ticket_medio,
            COUNT(*) FILTER (WHERE situacao IN ('Cancelado','Cancelada')) AS cancelamentos
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
    """, (data_ini, data_fim))[0]

    novos_clientes = scalar(f"""
        SELECT COUNT(*) FROM tiny_clientes
        WHERE {dc('data_criacao')} BETWEEN %s AND %s
    """, (data_ini, data_fim)) or 0

    assinantes = scalar("SELECT COUNT(*) FROM guru_assinaturas WHERE last_status = 'active'") or 0

    return {**vendas, "novos_clientes": novos_clientes, "assinantes_ativos": assinantes}

@app.get("/api/geral/faturamento-diario")
def geral_fat_diario(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            {dc('data_pedido')} AS data,
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS faturamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 1
    """, (data_ini, data_fim))

@app.get("/api/geral/por-situacao")
def geral_situacao(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT situacao, COUNT(*) AS total
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 2 DESC
    """, (data_ini, data_fim))

@app.get("/api/geral/top-produtos")
def geral_top_produtos(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 10):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            vi.descricao,
            SUM(vi.quantidade) AS qtd,
            SUM(vi.valor_total) AS receita
        FROM tiny_vendas_itens vi
        JOIN tiny_vendas v ON v.id_tiny = vi.id_pedido
        WHERE {dc('v.data_pedido')} BETWEEN %s AND %s
          AND v.situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1 ORDER BY 3 DESC LIMIT %s
    """, (data_ini, data_fim, limit))

@app.get("/api/geral/mensal-comparativo")
def geral_mensal(anos: int = 2):
    rows = q(f"""
        SELECT
            EXTRACT(YEAR FROM {dc('data_pedido')})::int AS ano,
            EXTRACT(MONTH FROM {dc('data_pedido')})::int AS mes,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS faturamento,
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos
        FROM tiny_vendas
        WHERE {dc('data_pedido')} >= (CURRENT_DATE - INTERVAL '{anos} years')
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    return rows

@app.get("/api/geral/heatmap")
def geral_heatmap(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            EXTRACT(DOW FROM {dc('data_pedido')})::int AS dia_semana,
            EXTRACT(HOUR FROM synced_at::timestamp)::int AS hora,
            COUNT(*) AS pedidos
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        GROUP BY 1, 2 ORDER BY 1, 2
    """, (data_ini, data_fim))

# ─────────────────────────────────────────────
# VENDAS
# ─────────────────────────────────────────────
@app.get("/api/vendas/kpis")
def vendas_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS faturamento,
            COALESCE(SUM(total_produtos) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS total_produtos,
            COALESCE(SUM(valor_frete) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS total_frete,
            COALESCE(SUM(valor_desconto) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS total_desconto,
            COALESCE(AVG(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS ticket_medio,
            COUNT(*) FILTER (WHERE situacao IN ('Cancelado','Cancelada')) AS cancelamentos,
            ROUND(100.0 * COUNT(*) FILTER (WHERE situacao IN ('Cancelado','Cancelada')) / NULLIF(COUNT(*), 0), 1) AS pct_cancelamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
    """, (data_ini, data_fim))[0]

@app.get("/api/vendas/por-canal")
def vendas_canal(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(canal_venda,''), NULLIF(ecommerce,''), 'Direto') AS canal,
            COUNT(*) AS pedidos,
            SUM(total_pedido) AS faturamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1 ORDER BY 3 DESC
    """, (data_ini, data_fim))

@app.get("/api/vendas/por-pagamento")
def vendas_pagamento(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(meio_pagamento,''), NULLIF(forma_pagamento,''), 'Não informado') AS pagamento,
            COUNT(*) AS pedidos,
            SUM(total_pedido) AS faturamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1 ORDER BY 3 DESC LIMIT 10
    """, (data_ini, data_fim))

@app.get("/api/vendas/mensal")
def vendas_mensal(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            TO_CHAR({dc('data_pedido')}, 'YYYY-MM') AS mes,
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS faturamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 1
    """, (data_ini, data_fim))

@app.get("/api/vendas/ultimas")
def vendas_ultimas(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 50):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            {dc('data_pedido')} AS data,
            numero, numero_ecommerce, nome_cliente,
            situacao, total_pedido, forma_envio,
            COALESCE(NULLIF(meio_pagamento,''), forma_pagamento) AS pagamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        ORDER BY {dc('data_pedido')} DESC, id_tiny DESC
        LIMIT %s
    """, (data_ini, data_fim, limit))

# ─────────────────────────────────────────────
# CLIENTES
# ─────────────────────────────────────────────
@app.get("/api/clientes/kpis")
def clientes_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)

    total = scalar("SELECT COUNT(*) FROM tiny_clientes") or 0
    novos = scalar(f"""
        SELECT COUNT(*) FROM tiny_clientes
        WHERE {dc('data_criacao')} BETWEEN %s AND %s
    """, (data_ini, data_fim)) or 0

    recorrentes = scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT id_contato FROM tiny_vendas
            WHERE {dc('data_pedido')} BETWEEN %s AND %s
              AND situacao NOT IN ('Cancelado','Cancelada')
            GROUP BY id_contato HAVING COUNT(*) > 1
        ) t
    """, (data_ini, data_fim)) or 0

    sem_compra_90 = scalar(f"""
        SELECT COUNT(DISTINCT id_contato) FROM tiny_vendas
        WHERE {dc('data_pedido')} < (CURRENT_DATE - INTERVAL '90 days')
          AND id_contato NOT IN (
              SELECT id_contato FROM tiny_vendas
              WHERE {dc('data_pedido')} >= (CURRENT_DATE - INTERVAL '90 days')
          )
    """) or 0

    return {
        "total": total, "novos": novos,
        "recorrentes": recorrentes, "sem_compra_90": sem_compra_90
    }

@app.get("/api/clientes/novos-mensal")
def clientes_novos_mensal():
    return q(f"""
        SELECT
            TO_CHAR({dc('data_criacao')}, 'YYYY-MM') AS mes,
            COUNT(*) AS novos
        FROM tiny_clientes
        WHERE {dc('data_criacao')} >= (CURRENT_DATE - INTERVAL '18 months')
        GROUP BY 1 ORDER BY 1
    """)

@app.get("/api/clientes/por-uf")
def clientes_uf(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(uf_cliente,''), 'N/D') AS uf,
            COUNT(DISTINCT id_contato) AS clientes,
            SUM(total_pedido) AS faturamento
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1 ORDER BY 3 DESC LIMIT 20
    """, (data_ini, data_fim))

@app.get("/api/clientes/frequencia")
def clientes_frequencia(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            CASE
                WHEN compras = 1 THEN '1 compra'
                WHEN compras = 2 THEN '2 compras'
                WHEN compras = 3 THEN '3 compras'
                WHEN compras BETWEEN 4 AND 6 THEN '4-6 compras'
                ELSE '7+ compras'
            END AS faixa,
            COUNT(*) AS clientes
        FROM (
            SELECT id_contato, COUNT(*) AS compras
            FROM tiny_vendas
            WHERE {dc('data_pedido')} BETWEEN %s AND %s
              AND situacao NOT IN ('Cancelado','Cancelada')
            GROUP BY id_contato
        ) t
        GROUP BY 1 ORDER BY MIN(compras)
    """, (data_ini, data_fim))

@app.get("/api/clientes/top")
def clientes_top(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 20):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            nome_cliente,
            COUNT(*) AS pedidos,
            SUM(total_pedido) AS faturamento,
            AVG(total_pedido) AS ticket_medio,
            MAX({dc('data_pedido')}) AS ultima_compra
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY nome_cliente
        ORDER BY faturamento DESC LIMIT %s
    """, (data_ini, data_fim, limit))

# ─────────────────────────────────────────────
# PRODUTOS
# ─────────────────────────────────────────────
@app.get("/api/produtos/kpis")
def produtos_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            SUM(vi.quantidade) AS unidades_vendidas,
            COUNT(DISTINCT vi.codigo_produto) AS skus_vendidos,
            SUM(vi.valor_total) AS receita_total,
            AVG(vi.valor_unitario) AS preco_medio
        FROM tiny_vendas_itens vi
        JOIN tiny_vendas v ON v.id_tiny = vi.id_pedido
        WHERE {dc('v.data_pedido')} BETWEEN %s AND %s
          AND v.situacao NOT IN ('Cancelado','Cancelada')
    """, (data_ini, data_fim))[0]

@app.get("/api/produtos/top")
def produtos_top(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 20):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            vi.descricao,
            vi.codigo_produto,
            SUM(vi.quantidade) AS qtd,
            SUM(vi.valor_total) AS receita,
            AVG(vi.valor_unitario) AS preco_medio
        FROM tiny_vendas_itens vi
        JOIN tiny_vendas v ON v.id_tiny = vi.id_pedido
        WHERE {dc('v.data_pedido')} BETWEEN %s AND %s
          AND v.situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT %s
    """, (data_ini, data_fim, limit))

@app.get("/api/produtos/curva-abc")
def produtos_abc(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        WITH ranked AS (
            SELECT
                vi.descricao,
                SUM(vi.valor_total) AS receita,
                ROW_NUMBER() OVER (ORDER BY SUM(vi.valor_total) DESC) AS rank
            FROM tiny_vendas_itens vi
            JOIN tiny_vendas v ON v.id_tiny = vi.id_pedido
            WHERE {dc('v.data_pedido')} BETWEEN %s AND %s
              AND v.situacao NOT IN ('Cancelado','Cancelada')
            GROUP BY 1
        ),
        total AS (SELECT SUM(receita) AS total FROM ranked)
        SELECT
            r.rank, r.descricao, r.receita,
            ROUND(100.0 * SUM(r2.receita) / t.total, 2) AS acumulado_pct,
            CASE
                WHEN SUM(r2.receita) / t.total <= 0.8 THEN 'A'
                WHEN SUM(r2.receita) / t.total <= 0.95 THEN 'B'
                ELSE 'C'
            END AS curva
        FROM ranked r
        JOIN ranked r2 ON r2.rank <= r.rank
        CROSS JOIN total t
        GROUP BY r.rank, r.descricao, r.receita, t.total
        ORDER BY r.rank
        LIMIT 100
    """, (data_ini, data_fim))

# ─────────────────────────────────────────────
# ESTOQUE
# ─────────────────────────────────────────────
@app.get("/api/estoque/kpis")
def estoque_kpis():
    return q("""
        SELECT
            COUNT(*) FILTER (WHERE estoque_atual > 0) AS produtos_com_estoque,
            COUNT(*) FILTER (WHERE estoque_atual = 0 OR estoque_atual IS NULL) AS produtos_zerados,
            COUNT(*) FILTER (WHERE estoque_atual > 0 AND estoque_atual <= 5) AS criticos,
            COALESCE(SUM(estoque_atual * preco) FILTER (WHERE estoque_atual > 0), 0) AS valor_venda_estoque,
            COALESCE(SUM(estoque_atual * preco_custo) FILTER (WHERE estoque_atual > 0 AND preco_custo > 0), 0) AS valor_custo_estoque
        FROM tiny_produtos
        WHERE situacao = 'Ativo'
    """)[0]

@app.get("/api/estoque/top-custo")
def estoque_top_custo(limit: int = 20):
    return q("""
        SELECT
            nome, codigo,
            estoque_atual,
            preco_custo,
            preco,
            COALESCE(estoque_atual * preco_custo, 0) AS custo_total,
            COALESCE(estoque_atual * preco, 0) AS valor_venda
        FROM tiny_produtos
        WHERE situacao = 'Ativo' AND estoque_atual > 0 AND preco_custo > 0
        ORDER BY custo_total DESC LIMIT %s
    """, (limit,))

@app.get("/api/estoque/curva-abc")
def estoque_abc():
    return q("""
        WITH ranked AS (
            SELECT
                nome,
                COALESCE(estoque_atual * preco_custo, 0) AS custo_total,
                ROW_NUMBER() OVER (ORDER BY COALESCE(estoque_atual * preco_custo, 0) DESC) AS rank
            FROM tiny_produtos
            WHERE situacao = 'Ativo' AND estoque_atual > 0 AND preco_custo > 0
        ),
        total AS (SELECT SUM(custo_total) AS total FROM ranked)
        SELECT
            r.rank, r.nome, r.custo_total,
            ROUND(100.0 * SUM(r2.custo_total) / NULLIF(t.total,0), 2) AS acumulado_pct,
            CASE
                WHEN SUM(r2.custo_total) / NULLIF(t.total,0) <= 0.8 THEN 'A'
                WHEN SUM(r2.custo_total) / NULLIF(t.total,0) <= 0.95 THEN 'B'
                ELSE 'C'
            END AS curva
        FROM ranked r
        JOIN ranked r2 ON r2.rank <= r.rank
        CROSS JOIN total t
        GROUP BY r.rank, r.nome, r.custo_total, t.total
        ORDER BY r.rank LIMIT 80
    """)

@app.get("/api/estoque/encalhados")
def estoque_encalhados(dias: int = 60, limit: int = 30):
    return q(f"""
        SELECT
            p.nome, p.codigo,
            p.estoque_atual,
            p.preco_custo,
            COALESCE(p.estoque_atual * p.preco_custo, 0) AS custo_parado,
            MAX({dc('v.data_pedido')}) AS ultima_venda,
            (CURRENT_DATE - MAX({dc('v.data_pedido')}))::int AS dias_sem_venda
        FROM tiny_produtos p
        LEFT JOIN tiny_vendas_itens vi ON vi.codigo_produto = p.codigo
        LEFT JOIN tiny_vendas v ON v.id_tiny = vi.id_pedido
            AND v.situacao NOT IN ('Cancelado','Cancelada')
        WHERE p.situacao = 'Ativo' AND p.estoque_atual > 0
        GROUP BY p.nome, p.codigo, p.estoque_atual, p.preco_custo
        HAVING MAX({dc('v.data_pedido')}) < (CURRENT_DATE - INTERVAL '{dias} days')
            OR MAX({dc('v.data_pedido')}) IS NULL
        ORDER BY custo_parado DESC LIMIT %s
    """, (limit,))

# ─────────────────────────────────────────────
# FRETE
# ─────────────────────────────────────────────
@app.get("/api/frete/kpis")
def frete_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')) AS pedidos,
            COALESCE(AVG(valor_frete) FILTER (WHERE valor_frete > 0 AND situacao NOT IN ('Cancelado','Cancelada')), 0) AS frete_medio,
            COALESCE(SUM(valor_frete) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS total_frete,
            COUNT(*) FILTER (WHERE valor_frete = 0 AND situacao NOT IN ('Cancelado','Cancelada')) AS frete_gratis,
            ROUND(100.0 * COUNT(*) FILTER (WHERE valor_frete = 0 AND situacao NOT IN ('Cancelado','Cancelada'))
                / NULLIF(COUNT(*) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0), 1) AS pct_frete_gratis
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
    """, (data_ini, data_fim))[0]

@app.get("/api/frete/por-envio")
def frete_envio(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(forma_envio,''), 'Não informado') AS forma_envio,
            COUNT(*) AS pedidos,
            AVG(valor_frete) AS frete_medio,
            SUM(valor_frete) AS total_frete
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """, (data_ini, data_fim))

@app.get("/api/frete/por-uf")
def frete_uf(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(uf_cliente,''), 'N/D') AS uf,
            COUNT(*) AS pedidos,
            AVG(valor_frete) AS frete_medio
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
          AND situacao NOT IN ('Cancelado','Cancelada')
          AND valor_frete > 0
        GROUP BY 1 ORDER BY 3 DESC LIMIT 27
    """, (data_ini, data_fim))

@app.get("/api/frete/rastreios")
def frete_rastreios():
    return q("""
        SELECT
            r.codigo_rastreio,
            r.status_atual,
            r.ultima_verificacao,
            r.dt_prevista,
            r.forma_frete,
            v.nome_cliente,
            v.uf_cliente,
            v.total_pedido
        FROM rastreio_pedidos r
        LEFT JOIN tiny_vendas v ON v.id_tiny = r.id_venda_tiny
        WHERE r.entregue = 0
        ORDER BY r.ultima_verificacao DESC
        LIMIT 100
    """)

@app.get("/api/frete/status-rastreio")
def frete_status_rastreio():
    return q("""
        SELECT
            CASE
                WHEN status_atual ILIKE '%entregue%' THEN 'Entregue'
                WHEN status_atual ILIKE '%saiu para%' OR status_atual ILIKE '%saiu p/%' THEN 'Saiu p/ entrega'
                WHEN status_atual ILIKE '%em trânsito%' OR status_atual ILIKE '%transferência%' THEN 'Em trânsito'
                WHEN status_atual ILIKE '%postado%' THEN 'Postado'
                ELSE 'Outros'
            END AS status,
            COUNT(*) AS total
        FROM rastreio_pedidos
        WHERE entregue = 0
        GROUP BY 1 ORDER BY 2 DESC
    """)

# ─────────────────────────────────────────────
# FINANCEIRO
# ─────────────────────────────────────────────
@app.get("/api/financeiro/kpis")
def financeiro_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)

    vendas = q(f"""
        SELECT
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS receita_bruta,
            COALESCE(SUM(valor_desconto) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS descontos,
            COALESCE(SUM(total_pedido - valor_desconto) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS receita_liquida
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
    """, (data_ini, data_fim))[0]

    contas = q(f"""
        SELECT
            COALESCE(SUM(valor) FILTER (WHERE {dc('data_vencimento')} BETWEEN %s AND %s), 0) AS a_pagar_periodo,
            COALESCE(SUM(valor) FILTER (WHERE {dc('data_vencimento')} < CURRENT_DATE AND saldo > 0), 0) AS vencidas,
            COALESCE(SUM(valor) FILTER (WHERE {dc('data_vencimento')} BETWEEN CURRENT_DATE AND CURRENT_DATE + 30 AND saldo > 0), 0) AS proximos_30d
        FROM tiny_contas_pagar_pu
    """, (data_ini, data_fim))[0]

    assinantes = scalar("SELECT COUNT(*) FROM guru_assinaturas WHERE last_status = 'active'") or 0
    mrr = scalar(f"SELECT COALESCE(SUM(valor_liquido),0) FROM guru_vendas WHERE {dc('confirmed_at')} >= DATE_TRUNC('month', CURRENT_DATE)::date") or 0

    return {**vendas, **contas, "assinantes": assinantes, "mrr": mrr}

@app.get("/api/financeiro/receita-diaria")
def financeiro_receita_diaria(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            {dc('data_pedido')} AS data,
            COALESCE(SUM(total_pedido) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS receita,
            COALESCE(SUM(valor_desconto) FILTER (WHERE situacao NOT IN ('Cancelado','Cancelada')), 0) AS desconto
        FROM tiny_vendas
        WHERE {dc('data_pedido')} BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 1
    """, (data_ini, data_fim))

@app.get("/api/financeiro/guru-mensal")
def financeiro_guru():
    return q("""
        SELECT
            DATE_TRUNC('month', confirmed_at::timestamp)::date AS mes,
            COUNT(*) AS vendas,
            COALESCE(SUM(valor_liquido),0) AS receita
        FROM guru_vendas
        WHERE confirmed_at IS NOT NULL AND confirmed_at::text != ''
        GROUP BY 1 ORDER BY 1
    """)

@app.get("/api/financeiro/assinaturas-mensal")
def financeiro_assinaturas():
    return q(f"""
        SELECT
            DATE_TRUNC('month', started_at::timestamp)::date AS mes,
            COUNT(*) FILTER (WHERE last_status = 'active') AS novos,
            COUNT(*) FILTER (WHERE last_status = 'canceled') AS cancelados
        FROM guru_assinaturas
        WHERE started_at IS NOT NULL AND started_at::text != ''
        GROUP BY 1 ORDER BY 1
    """)

@app.get("/api/financeiro/contas-pagar")
def financeiro_contas(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q(f"""
        SELECT
            COALESCE(NULLIF(competencia,''), 'N/D') AS categoria,
            SUM(valor) AS total,
            COUNT(*) AS qtd
        FROM tiny_contas_pagar_pu
        WHERE {dc('data_vencimento')} BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """, (data_ini, data_fim))

# ─────────────────────────────────────────────
# SEO — Google Search Console
# ─────────────────────────────────────────────
def ensure_gsc_tables():
    """Create GSC tables if they don't exist."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gsc_queries (
                    id SERIAL PRIMARY KEY,
                    data DATE NOT NULL,
                    query TEXT NOT NULL,
                    page TEXT,
                    country TEXT DEFAULT 'BRA',
                    device TEXT DEFAULT 'DESKTOP',
                    clicks INT DEFAULT 0,
                    impressions INT DEFAULT 0,
                    ctr NUMERIC(6,4) DEFAULT 0,
                    position NUMERIC(6,2) DEFAULT 0,
                    synced_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(data, query, page, country, device)
                );
                CREATE TABLE IF NOT EXISTS gsc_pages (
                    id SERIAL PRIMARY KEY,
                    data DATE NOT NULL,
                    page TEXT NOT NULL,
                    clicks INT DEFAULT 0,
                    impressions INT DEFAULT 0,
                    ctr NUMERIC(6,4) DEFAULT 0,
                    position NUMERIC(6,2) DEFAULT 0,
                    synced_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(data, page)
                );
                CREATE INDEX IF NOT EXISTS idx_gsc_queries_data ON gsc_queries(data);
                CREATE INDEX IF NOT EXISTS idx_gsc_queries_query ON gsc_queries(query);
                CREATE INDEX IF NOT EXISTS idx_gsc_pages_data ON gsc_pages(data);
                CREATE INDEX IF NOT EXISTS idx_gsc_pages_page ON gsc_pages(page);
            """)
            conn.commit()

@app.on_event("startup")
async def startup():
    ensure_gsc_tables()


@app.get("/api/seo/kpis")
def seo_kpis(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    current = q("""
        SELECT
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(impressions), 0) AS impressions,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(100.0 * SUM(clicks)::numeric / SUM(impressions), 2)
                ELSE 0 END AS ctr,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(SUM(position * impressions)::numeric / SUM(impressions), 1)
                ELSE 0 END AS avg_position,
            COUNT(DISTINCT query) AS total_queries,
            COUNT(DISTINCT page) AS total_pages
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
    """, (data_ini, data_fim))
    if not current:
        return {"clicks": 0, "impressions": 0, "ctr": 0, "avg_position": 0,
                "total_queries": 0, "total_pages": 0,
                "clicks_prev": 0, "impressions_prev": 0}

    # Previous period for comparison
    days = (date.fromisoformat(data_fim) - date.fromisoformat(data_ini)).days + 1
    prev_fim = (date.fromisoformat(data_ini) - timedelta(days=1)).isoformat()
    prev_ini = (date.fromisoformat(data_ini) - timedelta(days=days)).isoformat()
    prev = q("""
        SELECT
            COALESCE(SUM(clicks), 0) AS clicks_prev,
            COALESCE(SUM(impressions), 0) AS impressions_prev
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
    """, (prev_ini, prev_fim))

    return {**current[0], **(prev[0] if prev else {"clicks_prev": 0, "impressions_prev": 0})}


@app.get("/api/seo/clicks-diario")
def seo_clicks_diario(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q("""
        SELECT
            data,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 1
    """, (data_ini, data_fim))


@app.get("/api/seo/top-queries")
def seo_top_queries(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 30):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q("""
        SELECT
            query,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(100.0 * SUM(clicks)::numeric / SUM(impressions), 2)
                ELSE 0 END AS ctr,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(SUM(position * impressions)::numeric / SUM(impressions), 1)
                ELSE 0 END AS avg_position
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 2 DESC LIMIT %s
    """, (data_ini, data_fim, limit))


@app.get("/api/seo/top-pages")
def seo_top_pages(data_ini: Optional[str] = None, data_fim: Optional[str] = None, limit: int = 20):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q("""
        SELECT
            page,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(100.0 * SUM(clicks)::numeric / SUM(impressions), 2)
                ELSE 0 END AS ctr,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(SUM(position * impressions)::numeric / SUM(impressions), 1)
                ELSE 0 END AS avg_position
        FROM gsc_pages
        WHERE data BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 2 DESC LIMIT %s
    """, (data_ini, data_fim, limit))


@app.get("/api/seo/por-device")
def seo_por_device(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q("""
        SELECT
            device,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 2 DESC
    """, (data_ini, data_fim))


@app.get("/api/seo/posicao-diaria")
def seo_posicao_diaria(data_ini: Optional[str] = None, data_fim: Optional[str] = None):
    data_ini, data_fim = defaults(data_ini, data_fim)
    return q("""
        SELECT
            data,
            CASE WHEN SUM(impressions) > 0
                THEN ROUND(SUM(position * impressions)::numeric / SUM(impressions), 1)
                ELSE 0 END AS avg_position,
            SUM(clicks) AS clicks
        FROM gsc_queries
        WHERE data BETWEEN %s AND %s
        GROUP BY 1 ORDER BY 1
    """, (data_ini, data_fim))


@app.get("/api/seo/queries-mensal")
def seo_queries_mensal():
    return q("""
        SELECT
            TO_CHAR(data, 'YYYY-MM') AS mes,
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            COUNT(DISTINCT query) AS queries
        FROM gsc_queries
        WHERE data >= (CURRENT_DATE - INTERVAL '12 months')
        GROUP BY 1 ORDER BY 1
    """)


@app.post("/api/seo/sync")
def seo_sync(days: int = 7, secret: Optional[str] = None):
    """Run GSC sync from within the container. Requires SYNC_SECRET env var match."""
    expected = os.getenv("SYNC_SECRET", "pu-seo-sync-2024")
    if secret != expected:
        return {"error": "unauthorized"}

    import json as _json
    creds_json = os.getenv("GSC_CREDENTIALS_JSON")
    if not creds_json:
        return {"error": "GSC_CREDENTIALS_JSON not set"}

    try:
        from google.oauth2 import service_account as sa
        from googleapiclient.discovery import build as gbuild
        from psycopg2.extras import execute_values

        site_url = os.getenv("GSC_SITE_URL", "https://www.papelariaunicornio.com.br")
        scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
        info = _json.loads(creds_json)
        credentials = sa.Credentials.from_service_account_info(info, scopes=scopes)
        service = gbuild("searchconsole", "v1", credentials=credentials)

        end_date = (date.today() - timedelta(days=1)).isoformat()
        start_date = (date.today() - timedelta(days=days)).isoformat()

        def fetch_gsc(dims, row_limit=25000):
            all_rows, start_row = [], 0
            while True:
                resp = service.searchanalytics().query(siteUrl=site_url, body={
                    "startDate": start_date, "endDate": end_date,
                    "dimensions": dims, "rowLimit": row_limit, "startRow": start_row,
                }).execute()
                rows = resp.get("rows", [])
                if not rows:
                    break
                all_rows.extend(rows)
                start_row += len(rows)
                if len(rows) < row_limit:
                    break
            return all_rows

        with db() as conn:
            # Queries
            rows = fetch_gsc(["date", "query", "page", "country", "device"])
            if rows:
                vals = [(r["keys"][0], r["keys"][1], r["keys"][2], r["keys"][3], r["keys"][4],
                         r["clicks"], r["impressions"], round(r["ctr"], 4), round(r["position"], 2))
                        for r in rows]
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO gsc_queries (data, query, page, country, device, clicks, impressions, ctr, position, synced_at)
                        VALUES %s
                        ON CONFLICT (data, query, page, country, device)
                        DO UPDATE SET clicks=EXCLUDED.clicks, impressions=EXCLUDED.impressions,
                                      ctr=EXCLUDED.ctr, position=EXCLUDED.position, synced_at=NOW()
                    """, vals, page_size=500)
                conn.commit()
                total_q = len(vals)
            else:
                total_q = 0

            # Pages
            rows = fetch_gsc(["date", "page"])
            if rows:
                vals = [(r["keys"][0], r["keys"][1],
                         r["clicks"], r["impressions"], round(r["ctr"], 4), round(r["position"], 2))
                        for r in rows]
                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO gsc_pages (data, page, clicks, impressions, ctr, position, synced_at)
                        VALUES %s
                        ON CONFLICT (data, page)
                        DO UPDATE SET clicks=EXCLUDED.clicks, impressions=EXCLUDED.impressions,
                                      ctr=EXCLUDED.ctr, position=EXCLUDED.position, synced_at=NOW()
                    """, vals, page_size=500)
                conn.commit()
                total_p = len(vals)
            else:
                total_p = 0

        return {"ok": True, "period": f"{start_date} → {end_date}", "queries": total_q, "pages": total_p}

    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)

