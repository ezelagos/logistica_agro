select
    producto,
    sum(cantidad) as total_cantidad
from {{ ref('logistica_silver') }}
group by producto
