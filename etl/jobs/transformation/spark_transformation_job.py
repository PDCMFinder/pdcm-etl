import sys

import etl.jobs.transformation.diagnosis_transformer_job
import etl.jobs.transformation.ethnicity_transformer_job
import etl.jobs.transformation.provider_group_transformer_job
import etl.jobs.transformation.provider_type_transformer_job
import etl.jobs.transformation.model_transformer_job
import etl.jobs.transformation.publication_group_transformer_job
import etl.jobs.transformation.contact_people_transformer_job
import etl.jobs.transformation.contact_form_transformer_job
import etl.jobs.transformation.source_database_transformer_job
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
import etl.jobs.transformation.accessibility_group_transformer_job
import etl.jobs.transformation.host_strain_transformer_job
import etl.jobs.transformation.project_group_transformer_job
import etl.jobs.transformation.treatment_transformer_job
import etl.jobs.transformation.response_transformer_job
import etl.jobs.transformation.molecular_characterization_type_transformer_job
import etl.jobs.transformation.platform_transformer_job
import etl.jobs.transformation.molecular_characterization_transformer_job
import etl.jobs.transformation.cna_molecular_data_transformer_job
import etl.jobs.transformation.cytogenetics_molecular_data_transformer_job
import etl.jobs.transformation.expression_molecular_data_transformer_job
import etl.jobs.transformation.mutation_marker_transformer_job
import etl.jobs.transformation.mutation_measurement_data_transformer_job
import etl.jobs.transformation.gene_marker_transformer_job
import etl.jobs.transformation.specimen_transformer_job
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

    elif entity_name == Constants.CONTACT_PEOPLE_ENTITY:
        etl.jobs.transformation.contact_people_transformer_job.main(args_without_entity)

    elif entity_name == Constants.CONTACT_FORM_ENTITY:
        etl.jobs.transformation.contact_form_transformer_job.main(args_without_entity)

    elif entity_name == Constants.SOURCE_DATABASE_ENTITY:
        etl.jobs.transformation.source_database_transformer_job.main(args_without_entity)

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

    elif entity_name == Constants.SPECIMEN_ENTITY:
        etl.jobs.transformation.specimen_transformer_job.main(args_without_entity)

    elif entity_name == Constants.ACCESSIBILITY_GROUP_ENTITY:
        etl.jobs.transformation.accessibility_group_transformer_job.main(args_without_entity)

    elif entity_name == Constants.HOST_STRAIN_ENTITY:
        etl.jobs.transformation.host_strain_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PROJECT_GROUP_ENTITY:
        etl.jobs.transformation.project_group_transformer_job.main(args_without_entity)

    elif entity_name == Constants.TREATMENT_ENTITY:
        etl.jobs.transformation.treatment_transformer_job.main(args_without_entity)

    elif entity_name == Constants.RESPONSE_ENTITY:
        etl.jobs.transformation.response_transformer_job.main(args_without_entity)

    elif entity_name == Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY:
        etl.jobs.transformation.molecular_characterization_type_transformer_job.main(args_without_entity)

    elif entity_name == Constants.PLATFORM_ENTITY:
        etl.jobs.transformation.platform_transformer_job.main(args_without_entity)

    elif entity_name == Constants.MOLECULAR_CHARACTERIZATION_ENTITY:
        etl.jobs.transformation.molecular_characterization_transformer_job.main(args_without_entity)

    elif entity_name == Constants.CNA_MOLECULAR_DATA_ENTITY:
        etl.jobs.transformation.cna_molecular_data_transformer_job.main(args_without_entity)

    elif entity_name == Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY:
        etl.jobs.transformation.cytogenetics_molecular_data_transformer_job.main(args_without_entity)

    elif entity_name == Constants.EXPRESSION_MOLECULAR_DATA_ENTITY:
        etl.jobs.transformation.expression_molecular_data_transformer_job.main(args_without_entity)

    elif entity_name == Constants.MUTATION_MARKER_ENTITY:
        etl.jobs.transformation.mutation_marker_transformer_job.main(args_without_entity)

    elif entity_name == Constants.MUTATION_MEASUREMENT_DATA_ENTITY:
        etl.jobs.transformation.mutation_measurement_data_transformer_job.main(args_without_entity)

    elif entity_name == Constants.GENE_MARKER_ENTITY:
        etl.jobs.transformation.gene_marker_transformer_job.main(args_without_entity)


if __name__ == "__main__":
    sys.exit(main1(sys.argv))
