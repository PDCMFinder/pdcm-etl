import sys

import etl.jobs.transformation.diagnosis_transformer_job
import etl.jobs.transformation.ethnicity_transformer_job
import etl.jobs.transformation.provider_group_transformer_job
import etl.jobs.transformation.provider_type_transformer_job
import etl.jobs.transformation.model_transformer_job
import etl.jobs.transformation.publication_group_transformer_job
import etl.jobs.transformation.quality_assurance_transformer_job
import etl.jobs.transformation.patient_transformer_job
import etl.jobs.transformation.tissue_transformer_job
import etl.jobs.transformation.tumour_type_transformer_job
import etl.jobs.transformation.patient_sample_transformer_job
import etl.jobs.transformation.xenograft_sample_transformer_job
import etl.jobs.transformation.patient_snapshot_transformer_job
import etl.jobs.transformation.engraftment_site_transformer_job
import etl.jobs.transformation.engraftment_type_transformer_job
import etl.jobs.transformation.engraftment_material_transformer_job
import etl.jobs.transformation.engraftment_sample_state_transformer_job
import etl.jobs.transformation.engraftment_sample_type_transformer_job

from etl.constants import Constants


def main1(argv):
    """
    Calls the needed job according to the entity
    :param list argv: the list elements should be:
                    [1]: Entity name
                    [2:]: Rest of parameters (the paths to the parquet files needed for the job).
                    Last parameter is always the output path
    """
    entity_name = argv[1]
    args_without_entity = argv[0:1] + argv[2:]

    if entity_name == Constants.DIAGNOSIS_ENTITY:
        etl.jobs.transformation.diagnosis_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ETHNICITY_ENTITY:
        etl.jobs.transformation.ethnicity_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PROVIDER_GROUP_ENTITY:
        etl.jobs.transformation.provider_group_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PROVIDER_TYPE_ENTITY:
        etl.jobs.transformation.provider_type_transformer_job.main(args_without_entity)

    elif entity_name == Constants.MODEL_ENTITY:
        etl.jobs.transformation.model_transformer_job.main(args_without_entity)

    elif entity_name == Constants.QUALITY_ASSURANCE_ENTITY:
        etl.jobs.transformation.quality_assurance_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PATIENT_ENTITY:
        etl.jobs.transformation.patient_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PUBLICATION_GROUP_ENTITY:
        etl.jobs.transformation.publication_group_transformer_job.main(args_without_entity)

    elif entity_name == Constants.TISSUE_ENTITY:
        etl.jobs.transformation.tissue_transformer_job.main(args_without_entity)

    elif entity_name == Constants.TUMOUR_TYPE_ENTITY:
        etl.jobs.transformation.tumour_type_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PATIENT_SAMPLE_ENTITY:
        etl.jobs.transformation.patient_sample_transformer_job.main(args_without_entity)

    elif entity_name == Constants.XENOGRAFT_SAMPLE_ENTITY:
        etl.jobs.transformation.xenograft_sample_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PATIENT_SNAPSHOT_ENTITY:
        etl.jobs.transformation.patient_snapshot_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ENGRAFTMENT_SITE_ENTITY:
        etl.jobs.transformation.engraftment_site_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ENGRAFTMENT_TYPE_ENTITY:
        etl.jobs.transformation.engraftment_type_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ENGRAFTMENT_MATERIAL_ENTITY:
        etl.jobs.transformation.engraftment_material_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY:
        etl.jobs.transformation.engraftment_sample_state_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY:
        etl.jobs.transformation.engraftment_sample_type_transformer_job.main(args_without_entity)


if __name__ == "__main__":
    sys.exit(main1(sys.argv))
