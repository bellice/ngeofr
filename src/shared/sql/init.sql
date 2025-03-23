CREATE OR REPLACE TABLE ngeofr AS
SELECT
    com.com_type::VARCHAR AS com_type,
    com.com_insee::VARCHAR AS com_insee,
    com_siren.com_siren::VARCHAR AS com_siren,
    com.com_nom::VARCHAR AS com_nom,
    reg.reg_insee::VARCHAR AS reg_insee,
    reg.reg_nom::VARCHAR AS reg_nom,
    reg.reg_cheflieu::VARCHAR AS reg_cheflieu,
    arr.arr_insee::VARCHAR AS arr_insee,
    arr.arr_nom::VARCHAR AS arr_nom,
    arr.arr_cheflieu::VARCHAR AS arr_cheflieu,
    dep.dep_insee::VARCHAR AS dep_insee,
    dep.dep_nom::VARCHAR AS dep_nom,
    dep.dep_cheflieu::VARCHAR AS dep_cheflieu,
    epci.epci_siren::VARCHAR AS epci_siren,
    epci.epci_nom::VARCHAR AS epci_nom,
    epci.epci_cheflieu::VARCHAR AS epci_cheflieu,
    epci.epci_interdep::BOOLEAN AS epci_interdep,
    epci.epci_naturejuridique::VARCHAR AS epci_naturejuridique,
    ept.ept_siren::VARCHAR AS ept_siren,
    ept.ept_nom::VARCHAR AS ept_nom,
    ept.ept_cheflieu::VARCHAR AS ept_cheflieu,
    ept.ept_naturejuridique::VARCHAR AS ept_naturejuridique,
    pop.pop_recens::INTEGER AS pop_recens
FROM table_com com
LEFT JOIN table_arr arr ON com.arr_insee = arr.arr_insee
LEFT JOIN table_dep dep ON com.dep_insee = dep.dep_insee
LEFT JOIN table_reg reg ON com.reg_insee = reg.reg_insee
LEFT JOIN table_com_siren com_siren ON com.com_insee = com_siren.com_insee
LEFT JOIN table_epci_perimetre epci ON com_siren.com_siren = epci.epci_membre_siren
LEFT JOIN table_ept_perimetre ept ON com_siren.com_siren = ept.ept_membre_siren
LEFT JOIN table_population pop ON com.com_type = pop.com_type AND com.com_insee = pop.com_insee AND com.com_nom = pop.com_nom
WHERE com.com_type = 'COM';