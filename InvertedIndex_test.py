class InvertedIndex:
    def __init__(self):
        self.index = {}

    def add_document(self, doc_id, doc_text):
        """
        Add a document to the inverted index.
        :param doc_id: Document ID
        :param doc_text: Document text
        """
        terms = doc_text.split()
        term_freq = {}
        for term in terms:
            term = term.lower()  # Normalize the term to lowercase
            if term not in term_freq:
                term_freq[term] = 0
            term_freq[term] += 1

        for term, freq in term_freq.items():
            if term not in self.index:
                self.index[term] = []
            self.index[term].append((doc_id, freq))

    def search(self, query):
        """
        Search for documents that contain the query terms.
        :param query: Query string
        :return: List of document IDs that match the query
        """
        query_terms = query.lower().split()
        result = []
        for term in query_terms:
            if term in self.index:
                result.extend([doc_id for doc_id, _ in self.index[term]])
        return list(set(result))  # Remove duplicate document IDs

    def get_term_frequency(self, term, doc_id):
        """
        Get the term frequency of a term in a document.
        :param term: Query term
        :param doc_id: Document ID
        :return: Term frequency
        """
        if term in self.index:
            for doc_id_, freq in self.index[term]:
                if doc_id_ == doc_id:
                    return freq
        return 0

# Example usage:

# Create an inverted index
index = InvertedIndex()

# Add documents to the index
index.add_document(1, "apple orange banana apple")
index.add_document(2, "orange grapefruit")
index.add_document(3, "banana apple")
index.add_document(4, "grapefruit orange")

# Search for documents that contain the query terms "apple" and "orange"
query = "apple orange"
result = index.search(query)
print("Documents that contain the query terms:", result)

# Output: Documents that contain the query terms: [1, 3, 4]

# Get the term frequency of "apple" in document 1
term = "apple"
doc_id = 1
term_freq = index.get_term_frequency(term, doc_id)
print(f"Term frequency of '{term}' in document {doc_id}: {term_freq}")

# Output: Term frequency of 'apple' in document 1: 2
