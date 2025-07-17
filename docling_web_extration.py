"""
Module to scrap data from websites and return on MarkDown format
"""
## docling dependency
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
import traceback

def scrap_data_using_docling(url:str, export_to_dict:bool, export_to_text:bool, *args, **kwargs) -> str:
    """
    Function to scrap data from websites and return on MarkDown format
    """
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True
    try:
        doc_converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
        result = doc_converter.convert(url)
        if export_to_dict:
            return result.document.export_to_dict()
        if export_to_text:
            return result.document.export_to_text()

        return result.document.export_to_markdown().replace('<!-- image -->', '\n')  ## remove the image tag from markdown
    except Exception as e:
        print(traceback.print_exc())
        return str(e)