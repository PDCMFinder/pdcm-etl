ALTER TABLE model_information
    ADD COLUMN IF NOT exists model_relationships JSON;

COMMENT ON COLUMN model_information.model_relationships IS 'Model relationships';

UPDATE  model_information
    SET model_relationships = 
    (
        SELECT to_jsonb(r) FROM 
        (
            SELECT 
                to_jsonb(pdcm_api.get_parents_tree(external_model_id)) as parents,
                to_jsonb(pdcm_api.get_children_tree(external_model_id)) as children
        ) r
    )
;

ALTER TABLE model_information
    ADD COLUMN IF NOT EXISTS has_relations  BOOLEAN;

COMMENT ON COLUMN model_information.has_relations IS 'Indicates if the model has parent(s) or children';

UPDATE model_information
    SET has_relations = 
    (
        CASE
          WHEN 
	          model_relationships->>'parents' is not null or 
 	          model_relationships->>'children' is not null 
          THEN true 
          ELSE false 
        END
    )
;