select a.nome, round(sum(a.valor * a.ativo * (a.percentual/100) * (1 - a.percentual_desconto/100)),2) as resultado
from
(
SELECT c.cliente_id, c.nome, ct.contrato_id, ct.ativo, sum(t.valor_total) as valor, ct.percentual, COALESCE(t.percentual_desconto,0) as percentual_desconto
FROM   cliente c 
       LEFT JOIN contrato ct 
         ON c.cliente_id = ct.cliente_id
       LEFT JOIN transacao t 
         ON ct.contrato_id = t.contrato_id
WHERE c.nome in ('Cliente A', 'Cliente D')
GROUP  BY c.cliente_id, c.nome,  ct.contrato_id, ct.contrato_id, ct.percentual, t.percentual_desconto, ct.ativo 
) a
GROUP  BY a.nome