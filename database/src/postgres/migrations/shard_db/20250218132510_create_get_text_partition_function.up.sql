CREATE OR REPLACE FUNCTION get_text_partition(value TEXT, partitions INT)
RETURNS INT AS $$
DECLARE
    hash_val numeric;
BEGIN
    hash_val := (
        (
            hashtextextended(value, 8816678312871386365)::numeric
            + 5305509591434766563
            + 18446744073709551616
        ) % 18446744073709551616
    );

    RETURN (hash_val % partitions)::int;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
