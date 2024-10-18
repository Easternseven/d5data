from datasets import load_dataset
import json
import re
from tqdm import tqdm
import time
import logging
import hashlib
from multiprocessing import Pool, cpu_count

PATTERNS = [
    re.compile(pattern, re.DOTALL) for pattern in [
        r'Headline:\s*"(.*?)"\s*Now answer this question:\s*(.*?)\?\s*(Yes|No)?',
        r'Headline:\s*(.*?)\s*Question:\s*(.*?)\?\s*(Yes|No)?',
        r'Please answer a question about the following headline:\s*"(.*?)"\s*(.*?)\?\s*No or Yes\?\s*(Yes|No)?',
        r'Read this headline:\s*"(.*?)"\s*Now answer this question:\s*"(.*?)"\s*Options:.*?-\s*.*?-\s*.*?(Yes|No)?',
        r'(.*?)\s*Q:\s*(.*?)\?\s*(Yes|No)?'
    ]
]

def extract_questions(text):
    extracted = []
    for pattern in PATTERNS:
        matches = pattern.findall(text)
        for match in matches:
            headline, question, answer = match
            extracted.append({
                "Headline": headline.strip(),
                "Question": question.strip(),
                "Answer": answer.strip() if answer else None
            })
    return extracted

def process_item(args):
    idx, item = args
    try:
        input_text = item['input']
        options = item['options']
        gold_index = item['gold_index']
        
        questions = extract_questions(input_text)
        
        qa_pairs = []
        for q_idx, qa in enumerate(questions):
            is_target = (q_idx == len(questions) - 1)
            
            qa_pair = {
                "id": hashlib.md5(f"{idx}_{q_idx}_{qa['Headline'][:50]}".encode()).hexdigest(),
                "Headline": qa['Headline'],
                "Question": qa['Question'],
                "IsTarget": is_target,
                "OriginalIndex": idx
            }
            
            if is_target:
                qa_pair["Options"] = options
                qa_pair["GoldIndex"] = gold_index
                qa_pair["Answer"] = options[gold_index]
            else:
                qa_pair["Answer"] = qa['Answer']
            
            qa_pairs.append(qa_pair)
        
        return qa_pairs
    except Exception as e:
        logging.error(f"Error processing item {idx}: {str(e)}")
        return []

def process_headline_dataset(dataset):
    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(pool.imap(process_item, enumerate(dataset)), total=len(dataset), desc="Processing items"))
    return [item for sublist in results for item in sublist]

def save_to_json(data, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    output_file = 'processed_headline_dataset.json'
    
    start_time = time.time()
    
    logging.info("Loading dataset...")
    dataset = load_dataset("AdaptLLM/finance-tasks", "Headline")
    
    logging.info("Processing dataset...")
    processed_data = process_headline_dataset(dataset['test'])
    
    logging.info("Saving results...")
    save_to_json(processed_data, output_file)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    logging.info(f"Total question-answer pairs extracted: {len(processed_data)}")
    logging.info(f"Processing time: {processing_time:.2f} seconds")

if __name__ == "__main__":
    main()