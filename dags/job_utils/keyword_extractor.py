import sparknlp
from sparknlp.base import DocumentAssembler, Pipeline, LightPipeline
from sparknlp.annotator import SentenceDetector, Tokenizer, YakeKeywordExtraction
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, FloatType
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_html(html_str):
    """Remove HTML tags from text."""
    if not html_str:
        return ""
    
    clean_r = re.compile('<.*?>')
    clean_text = re.sub(clean_r, ' ', html_str)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    return clean_text

class KeywordExtractor:
    """Extract keywords from job descriptions using Spark NLP."""
    
    def __init__(self, spark=None):
        """Initialize the keyword extractor."""
        self.spark = spark or sparknlp.start()
        self.pipeline = None
        self.model = None
        self.light_model = None
        self._initialize_pipeline()
    
    def _initialize_pipeline(self):
        """Initialize the Spark NLP pipeline."""
        # Document assembler
        document = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")
        
        # Sentence detector
        sentenceDetector = SentenceDetector() \
            .setInputCols(["document"]) \
            .setOutputCol("sentence")
        
        # Tokenizer
        tokenizer = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token") \
            .setContextChars(["(", ")", "?", "!", ".", ","])
        
        # Keyword extraction
        keywords = YakeKeywordExtraction() \
            .setInputCols(["token"]) \
            .setOutputCol("keywords") \
            .setMinNGrams(1) \
            .setMaxNGrams(3) \
            .setNKeywords(20) \
            .setThreshold(0.6)
        
        # Create pipeline
        self.pipeline = Pipeline(stages=[document, sentenceDetector, tokenizer, keywords])
        
        # Fit an empty DataFrame to get a model
        empty_df = self.spark.createDataFrame([[""]]).toDF("text")
        self.model = self.pipeline.fit(empty_df)
        
        # Create a light pipeline for faster processing
        self.light_model = LightPipeline(self.model)
    
    def extract_keywords_from_text(self, text):
        """Extract keywords from a text."""
        if not text:
            return []
        
        # Clean HTML
        clean_text = clean_html(text)
        
        # Extract keywords
        result = self.light_model.fullAnnotate(clean_text)[0]
        
        # Process keywords
        keywords = []
        if 'keywords' in result:
            for k in result['keywords']:
                score = float(k.metadata['score'])
                keywords.append({
                    "keyword": k.result,
                    "score": score,
                    "sentence": int(k.metadata['sentence'])
                })
            
            # Sort by score (lower is better in YAKE!)
            keywords.sort(key=lambda x: x['score'])
        
        return keywords
    
    def process_job_batch(self, job_ids, descriptions):
        """Process a batch of job descriptions."""
        results = []
        
        for job_id, description in zip(job_ids, descriptions):
            keywords = self.extract_keywords_from_text(description)
            
            results.append({
                "job_id": job_id,
                "keywords": keywords
            })
        
        return results
    
    def process_jobs_dataframe(self, df, id_col="job_id", text_col="description"):
        """Process a DataFrame of job descriptions."""
        # Clean text
        clean_udf = F.udf(clean_html, StringType())
        df_clean = df.withColumn("clean_text", clean_udf(F.col(text_col)))
        
        # Transform using the full pipeline
        result = self.model.transform(df_clean)
        
        # Define schema for keywords
        keyword_schema = ArrayType(
            StructType([
                StructField("keyword", StringType(), True),
                StructField("score", FloatType(), True),
                StructField("sentence", StringType(), True)
            ])
        )
        
        # Extract keywords as structured array
        extract_keywords_udf = F.udf(lambda keywords: [
            {"keyword": k.result, "score": float(k.metadata['score']), "sentence": k.metadata['sentence']}
            for k in keywords
        ], keyword_schema)
        
        result = result.withColumn("extracted_keywords", extract_keywords_udf(F.col("keywords")))
        
        # Select only necessary columns
        return result.select(
            F.col(id_col),
            F.col("extracted_keywords").alias("keywords")
        )
    
    def match_with_resume(self, job_keywords, resume_keywords):
        """Calculate match score between job and resume keywords."""
        if not job_keywords or not resume_keywords:
            return 0.0
        
        # Extract just the keyword strings
        job_terms = {kw["keyword"].lower() for kw in job_keywords}
        resume_terms = {kw["keyword"].lower() for kw in resume_keywords}
        
        # Calculate intersection
        matches = job_terms.intersection(resume_terms)
        
        # Calculate score as percentage of job terms matched
        if job_terms:
            return len(matches) / len(job_terms)
        
        return 0.0