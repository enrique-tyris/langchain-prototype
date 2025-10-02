SELECT
    uo.Cargo AS cargo_id,
    u.nombre,
    u.movil
FROM [usuarios obras] uo
LEFT JOIN usuariosnav u ON uo.Usuario = u.userid
WHERE [Nº proyecto] = ?