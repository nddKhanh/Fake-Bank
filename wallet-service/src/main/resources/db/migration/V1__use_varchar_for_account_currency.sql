-- Keep the database type aligned with the JPA String mapping.
ALTER TABLE accounts
    ALTER COLUMN currency TYPE VARCHAR(3);
