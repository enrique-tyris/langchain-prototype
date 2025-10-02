SELECT
    o.[Fecha acta recep_ definitiva] as recepcion,
    o.[Fecha adjudicación] as adjudicacion,
    o.[Fecha firma contrato] as firma_contrato,
    o.[Fecha acta de replanteo] as replanteo,
    o.[Fecha Fin Vigente] as fin_contrato
FROM [obras ayu] o
WHERE o.No_ = ?