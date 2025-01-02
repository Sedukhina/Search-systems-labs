import spacy
nlp = spacy.load("en_core_web_sm")

from yake import KeywordExtractor
kw_extractor = KeywordExtractor()

def GetKeywords(text):
    doc = nlp(text)
    individual_keywords = [token.text for token in doc if token.pos_ in ['NOUN', 'PROPN', 'VERB', 'ADJ']]
    
    multi_word_keywords = []

    for token in doc:
        # Adjective + Noun (e.g., "bold decision")
        if token.pos_ == "ADJ" and token.head.pos_ == "NOUN":
            multi_word_keywords.append(f"{token.text} {token.head.text}")
    
        # Verb + Object (e.g., "buying a startup")
        if token.pos_ == "VERB":
            for child in token.children:
                if child.dep_ in ["dobj", "obj", "prep", "pobj"]:  # Direct/indirect objects
                    multi_word_keywords.append(f"{token.text} {child.text}")
    
        # Subject + Verb (e.g., "Apple is buying")
        if token.dep_ in ["nsubj"] and token.head.pos_ == "VERB":
            multi_word_keywords.append(f"{token.text} {token.head.text}")

    all_keywords = list(set(individual_keywords + multi_word_keywords))
    lemmatized_keywords = [" ".join([token.lemma_ for token in nlp(keyword)]) for keyword in all_keywords]

    return lemmatized_keywords

def GetKeywords_depr(text):
    keywords = kw_extractor.extract_keywords(text.lower())
    if keywords:
        keywords = list(zip(*keywords))[0]
    else:
        keywords = text
    #lemmatized_keywords = [" ".join([token.lemma_ for token in nlp(keyword)]) for keyword in keywords]
    return keywords

def ExtractLocations(text):
    doc = nlp(text)
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]  # GPE = Geo-Political Entity
    return locations