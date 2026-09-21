import unittest

from embedding_cache import EmbeddingCache


class EmbeddingCacheTest(unittest.TestCase):
    def test_reuses_exact_text_for_same_model(self):
        cache = EmbeddingCache(2)
        cache.put("model-a", "same text", [0.1, 0.2])

        self.assertEqual((0.1, 0.2), cache.get("model-a", "same text"))
        self.assertIsNone(cache.get("model-b", "same text"))
        self.assertIsNone(cache.get("model-a", "different text"))

    def test_evicts_least_recently_used_entry(self):
        cache = EmbeddingCache(2)
        cache.put("model", "first", [1])
        cache.put("model", "second", [2])
        cache.get("model", "first")
        cache.put("model", "third", [3])

        self.assertIsNone(cache.get("model", "second"))
        self.assertEqual((1,), cache.get("model", "first"))

    def test_can_be_disabled(self):
        cache = EmbeddingCache(0)
        cache.put("model", "text", [1])
        self.assertIsNone(cache.get("model", "text"))


if __name__ == "__main__":
    unittest.main()
