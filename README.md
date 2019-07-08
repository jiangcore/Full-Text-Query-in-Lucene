# Full-Text-Query-in-Lucene

### This project is to build a Search Engine based on the MIMIC-III Notes Data which  allows users to locate ‘Discharge summary’ notes that satisfy user defined conditions. 
.
### Guides:
1.	Index all ‘Discharge summary’ notes in the MIMIC-III dataset with Lucene (Java, Python, or Solr). Information to be included in the index:
a.	Required: note ID, chart date, note text, hospital expire flag, all diagnoses ICD9 codes associated with the note via admission ID. For example, Admission A has 6 diagnoses ICD9 codes and 2 Discharge summary notes, then each of the 2 notes should be indexed with the 6 codes.
b.	Optional: any fields that help improve performance or functionality.

select n.row_id, n.charttime, n.text, a.hospital_expire_flag, 
d.icd9_code, i.short_title, n.category,  p.subject_id, a.hadm_id,d.seq_num
from patients p, admissions a, diagnoses_icd d, d_icd_diagnoses i , noteevents n
where p.subject_id = a.subject_id and a.hadm_id = d.hadm_id and d.icd9_code = i.icd9_code and d.hadm_id = n.hadm_id
 and n.category='Discharge summary'
              limit 1000

2.	Build a user interface that allows user to enter query conditions and returns an ID list (top 20) of satisfying notes. Interactive command line is fine. Web UI is preferred.  
a.	Lucene Query Syntax allowed for query strings.
b.	Assume users are not aware of field names in the Lucene index, while still allow users to search within one or an combination (with AND) of all the required information in 1.(a.). Hint: query condition does not have to be entered in just one step, or in one text input. 
c.	Query expansion for synonyms: suppose query string for note text is always a medical term, get all English synonyms of the input term from Consumer Health Vocabulary (CHV) in UMLS and include them in the query condition. Case-insensitive exact matching to locate term in UMLS is good enough.
For example, if a user searches for “lung cancer” in the note text condition, all the following terms should also be included in the search condition (limit to 30 synonyms): 
"cancer of lung", "cancer of the lung", "cancers lungs", "lung malignancy", "lung malignant tumors", "pulmonary cancer", "malignant lung neoplasm", "cancer pulmonary", "lung malignant tumours", "lungs cancer", "lung cancer", "lung cancers", "lung malignancies", "malignant neoplasm lung"
d.	(optional) Query expansion for child concepts: include up to 30 direct children defined in SNOMEDCT_US. Still use CHV as the term dictionary. Hint: term_text1CUI1 (match term in CHV)CUI2,3,4,…… (MRREF.REL=’CHD’ and MRREF.RELA=‘isa’ relation in SNOMED_US) term_text2,3,4,…… (match CUI in CHV)
e.	User should be able to control if to use query expansion C or D in the final query condition.


##  wrote a IndexFromDB.py to make the index files by querying from Postgresql database. Since the database is big as 54000 records of “Discharge Summary”, I just query the top 10000 records to test the codes in order to easily hand in the results files. This code works well.

## The following query Conditions work in  the search engine:
1.	Car crash in note text
2.	Car crash in note text and hospital_expire_flag=1 for the associated hospital admission
3.	162.9 in ICD codes.
4.	Note chartdate is between 2110-01-01 AND 2120-01-01 
5.	Brain cancer and its synonyms from CHV in note text
