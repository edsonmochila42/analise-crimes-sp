CREATE TABLE ocorrencias_sp_ssp (
  id INT(11) NOT NULL AUTO_INCREMENT,
  nome_departamento VARCHAR(255) NULL,
  nome_seccional VARCHAR(255) NULL,
  nome_delegacia VARCHAR(255) NULL,
  nome_municipio VARCHAR(50) NULL,
  num_bo VARCHAR(255) NULL,
  ano_bo INT(11) NULL,
  data_registro DATE NULL,
  data_ocorrencia_bo DATE NULL,
  hora_ocorrencia_bo TIME NULL,
  desc_periodo VARCHAR(20) NULL,
  bairro VARCHAR(100) NULL,
  logradouro VARCHAR(100) NULL,
  numero_logradouro INT(11) NULL,
  latitude DECIMAL(16, 13) NULL,
  longitude DECIMAL(16, 13) NULL,
  nome_delegacia_circunscricao VARCHAR(100) NULL,
  nome_departamento_circunscricao VARCHAR(100) NULL,
  nome_seccional_circunscricao VARCHAR(100) NULL,
  nome_municipio_circunscricao VARCHAR(100) NULL,
  rubrica VARCHAR(50) NULL,
  descr_conduta VARCHAR(100) NULL,
  natureza_apurada VARCHAR(100) NULL,
  mes_estatistica INT(11) NULL,
  ano_estatistica INT(11) NULL,
  descr_subtipolocal VARCHAR(50) NULL,
  
  -- Definição da Chave Primária
  PRIMARY KEY (id),
  
  -- Índices para otimizar buscas comuns
  INDEX idx_data_ocorrencia (data_ocorrencia_bo),
  INDEX idx_rubrica (rubrica),
  INDEX idx_municipio (nome_municipio)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;