import luigi

from etl.constants import Constants
from etl.workflow.readers.external_resources_reader import ReadModelIdsResources, ReadResources, \
    ReadDownloadedExternalResourcesFromCsv
from etl.workflow.readers.model_characterizations_conf_reader import ReadModelCharacterizationsConf
from etl.workflow.readers.mapping_rules_reader import ReadDiagnosisMappingsFromJson, ReadTreatmentMappingsFromJson
from etl.workflow.readers.markers_reader import ReadMarkerFromTsv
from etl.workflow.readers.ncit_reader import ReadOntologyFromObo
from etl.workflow.readers.ontolia_reader import ReadOntoliaFile
from etl.workflow.spark_reader import get_tsv_extraction_task_by_module, get_yaml_extraction_task_by_module
from etl.workflow.config import PdcmConfig


class ExtractModuleFromTsv(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def requires(self):
        return get_tsv_extraction_task_by_module(
            self.data_dir, self.providers, self.data_dir_out, self.module_name)


class ExtractModuleFromYaml(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def requires(self):
        return get_yaml_extraction_task_by_module(
            self.data_dir, list(self.providers), self.data_dir_out, self.module_name)


class ExtractSource(ExtractModuleFromYaml):
    module_name = Constants.SOURCE_MODULE


class ExtractPatient(ExtractModuleFromTsv):
    module_name = Constants.PATIENT_MODULE


class ExtractSample(ExtractModuleFromTsv):
    module_name = Constants.SAMPLE_MODULE


class ExtractSharing(ExtractModuleFromTsv):
    module_name = Constants.SHARING_MODULE


class ExtractModel(ExtractModuleFromTsv):
    module_name = Constants.MODEL_MODULE


class ExtractCellModel(ExtractModuleFromTsv):
    module_name = Constants.CELL_MODEL_MODULE


class ExtractModelValidation(ExtractModuleFromTsv):
    module_name = Constants.MODEL_VALIDATION_MODULE


class ExtractSamplePlatform(ExtractModuleFromTsv):
    module_name = Constants.SAMPLE_PLATFORM_MODULE


class ExtractMolecularMetadataSample(ExtractModuleFromTsv):
    module_name = Constants.MOLECULAR_DATA_SAMPLE_MODULE


class ExtractMolecularMetadataPlatform(ExtractModuleFromTsv):
    module_name = Constants.MOLECULAR_DATA_PLATFORM_MODULE


class ExtractMolecularMetadataPlatformWeb(ExtractModuleFromTsv):
    module_name = Constants.MOLECULAR_DATA_PLATFORM_WEB_MODULE


class ExtractDrugDosing(ExtractModuleFromTsv):
    module_name = Constants.DRUG_DOSING_MODULE


class ExtractPatientTreatment(ExtractModuleFromTsv):
    module_name = Constants.PATIENT_TREATMENT_MODULE


class ExtractCna(ExtractModuleFromTsv):
    module_name = Constants.CNA_MODULE


class ExtractBiomarker(ExtractModuleFromTsv):
    module_name = Constants.BIOMARKER_MODULE


class ExtractImmunemarker(ExtractModuleFromTsv):
    module_name = Constants.IMMUNEMARKER_MODULE

class ExtractExpression(ExtractModuleFromTsv):
    module_name = Constants.EXPRESSION_MODULE


class ExtractMutation(ExtractModuleFromTsv):
    module_name = Constants.MUTATION_MODULE


class ExtractGeneMarker(ReadMarkerFromTsv):
    module_name = Constants.GENE_MARKER_MODULE


class ExtractImageStudy(ExtractModuleFromTsv):
    module_name = Constants.IMAGE_STUDY_MODULE


class ExtractModelImage(ExtractModuleFromTsv):
    module_name = Constants.MODEL_IMAGE_MODULE


class ExtractOntology(ReadOntologyFromObo):
    module_name = Constants.ONTOLOGY_MODULE


class ExtractMappingDiagnosis(ReadDiagnosisMappingsFromJson):
    module_name = Constants.MAPPING_DIAGNOSIS_MODULE


class ExtractMappingTreatment(ReadTreatmentMappingsFromJson):
    module_name = Constants.MAPPING_TREATMENTS_MODULE


class ExtractOntolia(ReadOntoliaFile):
    module_name = Constants.REGIMENT_TO_TREATMENT_ENTITY


class ExtractExternalResources(ReadResources):
    module_name = Constants.EXTERNAL_RESOURCES_MODULE


class ExtractDownloadedResourcesData(ReadDownloadedExternalResourcesFromCsv):
    module_name = Constants.EXTERNAL_RESOURCES_DATA_MODULE

    def requires(self):
        return ExtractExternalResources()


class ExtractModelCharacterizationConf(ReadModelCharacterizationsConf):
    module_name = Constants.MODEL_CHARACTERIZATIONS_CONF_MODULE


class ExtractModelIdsResources(ReadModelIdsResources):
    module_name = Constants.MODEL_IDS_RESOURCES_MODULE


if __name__ == "__main__":
    luigi.run()
