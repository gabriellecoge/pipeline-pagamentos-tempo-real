-- Dados sintéticos: 20 contas, cada uma com 1 cartão (CARD-0001 ... CARD-0020).
-- Entram na "carga inicial" (snapshot) do conector, com op = 'r'.
INSERT INTO contas (id_conta, nome_titular, cidade, segmento, limite) VALUES
 ( 1, 'Ana',    'Vitória',        'varejo',  3000),
 ( 2, 'Bruno',  'Vila Velha',     'premium', 10000),
 ( 3, 'Carla',  'Serra',          'varejo',  1500),
 ( 4, 'Diego',  'Cariacica',      'varejo',  3000),
 ( 5, 'Elisa',  'São Paulo',      'premium', 10000),
 ( 6, 'Felipe', 'Rio de Janeiro', 'varejo',  5000),
 ( 7, 'Gabi',   'Vitória',        'varejo',  3000),
 ( 8, 'Heitor', 'Vila Velha',     'varejo',  1500),
 ( 9, 'Isis',   'Serra',          'premium', 10000),
 (10, 'João',   'Cariacica',      'varejo',  3000),
 (11, 'Karen',  'São Paulo',      'varejo',  5000),
 (12, 'Leo',    'Rio de Janeiro', 'varejo',  1500),
 (13, 'Marina', 'Vitória',        'premium', 10000),
 (14, 'Nina',   'Vila Velha',     'varejo',  3000),
 (15, 'Otávio', 'Serra',          'varejo',  5000),
 (16, 'Paula',  'Cariacica',      'varejo',  3000),
 (17, 'Rafa',   'São Paulo',      'varejo',  1500),
 (18, 'Sofia',  'Rio de Janeiro', 'premium', 10000),
 (19, 'Tiago',  'Vitória',        'varejo',  3000),
 (20, 'Vera',   'Vila Velha',     'varejo',  5000);
