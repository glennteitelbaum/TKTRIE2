#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <random>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <cstring>
#include "tktrie.h"

const std::vector<std::string> STRING_KEYS = {
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "I",
    "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
    "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
    "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
    "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
    "when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
    "people", "into", "year", "your", "good", "some", "could", "them", "see", "other",
    "than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
    "back", "after", "use", "two", "how", "our", "work", "first", "well", "way",
    "even", "new", "want", "because", "any", "these", "give", "day", "most", "us",
    "is", "was", "are", "been", "has", "had", "were", "said", "each", "made",
    "does", "did", "got", "may", "part", "find", "long", "down", "many", "before",
    "must", "through", "much", "where", "should", "very", "might", "being", "such", "more",
    "those", "never", "still", "world", "last", "own", "public", "while", "next", "less",
    "both", "life", "under", "same", "right", "here", "state", "place", "high", "every",
    "going", "another", "school", "number", "always", "however", "without", "great", "small", "between",
    "something", "important", "family", "government", "since", "system", "group", "children", "often", "money",
    "called", "water", "business", "almost", "program", "point", "hand", "having", "once", "away",
    "different", "night", "large", "order", "things", "already", "nothing", "possible", "second", "rather",
    "problem", "against", "though", "again", "person", "looking", "morning", "house", "during", "side",
    "power", "further", "young", "turned", "until", "start", "given", "working", "anything", "perhaps",
    "question", "reason", "early", "himself", "making", "enough", "better", "open", "show", "case",
    "seemed", "kind", "name", "read", "began", "believe", "several", "across", "office", "later",
    "usually", "city", "least", "story", "coming", "country", "social", "company", "close", "brought",
    "national", "service", "idea", "although", "behind", "true", "really", "home", "became", "become",
    "days", "taking", "within", "change", "available", "women", "level", "local", "mother", "doing",
    "development", "certain", "form", "whether", "door", "course", "member", "others", "center", "themselves",
    "best", "short", "white", "following", "around", "political", "face", "either", "using", "hours",
    "together", "interest", "whole", "community", "seen", "therefore", "along", "sure", "itself", "experience",
    "education", "keep", "light", "area", "study", "body", "started", "human", "nature", "president",
    "major", "sense", "result", "quite", "toward", "policy", "general", "control", "figure", "action",
    "process", "american", "provide", "based", "free", "support", "include", "church", "period", "future",
    "room", "common", "effect", "history", "probably", "need", "table", "special", "particular", "continue",
    "personal", "sometimes", "current", "complete", "everything", "actually", "individual", "seems", "care",
    "difficult", "simple", "economic", "research", "clear", "evidence", "recent", "strong", "private",
    "remember", "subject", "field", "position", "cannot", "class", "various", "outside", "report", "security",
    "building", "meeting", "value", "necessary", "likely", "return", "moment", "analysis", "central", "above",
    "force", "example", "similar", "thus", "stand", "type", "society", "entire", "decision", "north",
    "help", "mind", "everyone", "today", "federal", "terms", "view", "international", "according", "finally",
    "total", "love", "party", "single", "lost", "south", "information", "military", "section", "living",
    "provides", "main", "student", "role", "lines", "director", "knowledge", "court", "expected", "moved",
    "past", "standard", "attention", "especially", "basic", "half", "appeared", "allow", "treatment", "addition",
    "chance", "growth", "design", "previous", "management", "established", "wrong", "language", "board", "considered",
    "events", "approach", "range", "simply", "significant", "situation", "performance", "behavior", "difference",
    "words", "access", "hospital", "issues", "involved", "opportunity", "material", "training", "street", "modern",
    "higher", "blood", "response", "changes", "theory", "population", "inside", "pressure", "financial", "data",
    "effort", "developed", "meaning", "production", "method", "foreign", "physical", "amount", "traditional",
    "generally", "medical", "patient", "activity", "technology", "voice", "character", "environmental", "natural", "directly",
    "choice", "results", "project", "relationship", "needed", "function", "understanding", "factor", "operation",
    "concerned", "create", "consider", "black", "structure", "positive", "potential", "purpose", "paper", "successful",
    "western", "resource", "prepared", "learning", "serious", "middle", "space", "commission", "running", "model",
    "condition", "wall", "series", "culture", "official", "congress", "source", "described", "increase", "created",
    "science", "organization", "clearly", "network", "surface", "agreement", "agency", "works", "practice", "extent",
    "earlier", "recently", "additional", "challenge", "primary", "effective", "product", "presented", "suggested", "technical",
    "growing", "responsible", "written", "determined", "reality", "focus", "economy", "final", "professional", "rules",
    "strategy", "balance", "quality", "legal", "decade", "image", "responsibility", "applied", "critical", "religious",
    "workers", "movement", "capital", "associated", "direct", "defined", "values", "appropriate", "independent", "planning",
    "regular", "identify", "complex", "commercial", "limited", "demand", "energy", "alternative", "original", "conference",
    "article", "application", "principles", "insurance", "procedure", "capacity", "statement", "institution", "specific", "benefit",
    "democratic", "considerable", "normal", "industrial", "standards", "literature", "credit", "pattern", "content", "negative",
    "aspect", "coverage", "regional", "volume", "solutions", "element", "variable", "communication", "generation", "contract",
    "customer", "legislation", "assessment", "influence", "distinction", "distribution", "executive", "reduction", "selection", "definition",
    "perspective", "consequence", "version", "framework", "revolution", "protection", "resolution", "characteristic", "interpretation", "dimension",
    "representation", "contribution", "recognition", "acquisition", "investigation", "recommendation", "implementation", "consideration", "administration", "participation",
    "determination", "demonstration", "discrimination", "accommodation", "authentication", "authorization", "certification", "classification", "configuration", "consolidation",
    "construction", "consultation", "consumption", "contamination", "continuation", "conversation", "cooperation", "coordination", "corporation", "correlation",
    "customization", "deactivation", "decomposition", "decompression", "decentralization", "declaration", "decommission", "decoration", "dedication", "deformation",
    "degradation", "deliberation", "delineation", "denomination", "departmental", "depreciation", "deprivation", "derivation", "description",
    "desegregation", "desensitization", "designation", "desperation", "destination", "destruction", "deterioration", "detoxification", "devaluation",
    "devastation", "differentiation", "digitalization", "dimensionality", "disadvantageous", "disappointment", "disassociation", "discontinuation", "discouragement",
    "disenchantment", "disengagement", "disfigurement", "disillusionment", "disintegration", "dismemberment", "disorientation", "displacement", "disproportionate", "disqualification",
    "dissatisfaction", "dissemination", "dissertation", "distillation", "diversification", "documentation", "domestication", "dramatization", "duplication",
    "ecclesiastical", "economization", "editorialization", "effectuation", "effortlessness", "egalitarianism", "electrification", "electromagnetic", "electronically", "elementarily",
    "emancipation", "embellishment", "embodiment", "emotionalism", "empowerment", "encapsulation", "encouragement", "endangerment", "enlightenment", "entertainment",
    "enthusiastically", "entrepreneurial", "epistemological", "equalization", "equilibrium", "establishment", "evangelization", "evaporation", "exacerbation",
    "exaggeration", "examination", "exasperation", "excommunication", "exemplification", "exhaustiveness", "exhilaration", "existentialism", "experiential", "experimentation",
    "exploitation", "exploration", "exportation", "expropriation", "externalization", "extermination", "extraordinarily", "extraterrestrial", "extravagance", "fabrication",
    "facilitation", "falsification", "familiarization", "fantastically", "fascination", "featherweight", "federalization", "fertilization", "fictionalization", "fingerprinting",
    "firefighting", "flashforward", "flawlessness", "flexibilities", "flourishing", "fluctuation", "fluorescence", "forgetfulness", "formalization", "fortification",
    "fossilization", "fragmentation", "franchising", "fraternization", "freestanding", "friendliness", "frontrunners", "fruitfulness", "frustration", "fulfillment",
    "functionality", "fundamentalism", "fundraising", "galvanization", "generalization", "gentrification", "globalization", "glorification", "governmental", "gracelessness",
    "gradualness", "grandchildren", "grandparents", "gratification", "gravitational", "greenhouse", "groundbreaking", "groundskeeper", "guardianship", "habituation"
};

inline void uint64_to_key(uint64_t v, char* buf) {
    uint64_t be = __builtin_bswap64(v);
    std::memcpy(buf, &be, 8);
}

std::vector<std::string> generate_int_keys(size_t count) {
    std::vector<std::string> keys;
    keys.reserve(count);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    char buf[8];
    for (size_t i = 0; i < count; i++) {
        uint64_to_key(dist(rng), buf);
        keys.emplace_back(buf, 8);
    }
    return keys;
}

const std::vector<std::string> INT_KEYS = generate_int_keys(10000);

template<typename M>
class guarded_map {
    M data;
    mutable std::shared_mutex mtx;
public:
    bool contains(const typename M::key_type& key) const {
        std::shared_lock lock(mtx);
        return data.find(key) != data.end();
    }
    bool insert(const std::pair<typename M::key_type, typename M::mapped_type>& kv) {
        std::unique_lock lock(mtx);
        return data.insert(kv).second;
    }
    bool erase(const typename M::key_type& key) {
        std::unique_lock lock(mtx);
        return data.erase(key) > 0;
    }
};

template<typename K, typename V> using locked_map = guarded_map<std::map<K, V>>;
template<typename K, typename V> using locked_umap = guarded_map<std::unordered_map<K, V>>;

std::atomic<bool> stop{false};

template<typename Container, typename Keys>
double bench_find(Container& c, const Keys& keys, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) { for (const auto& k : keys) { c.contains(k); local++; } }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys, typename V>
double bench_insert(Container& c, const Keys& keys, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&, t]() {
            long long local = 0; int i = 0;
            while (!stop) { for (const auto& k : keys) { c.insert({k, (V)(t*10000 + i++)}); local++; } }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys>
double bench_erase(Container& c, const Keys& keys, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) { for (const auto& k : keys) { c.erase(k); local++; } }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys, typename V>
double bench_mixed_find(const Keys& keys, int find_threads, int write_threads, int ms) {
    Container c;
    for (size_t i = 0; i < keys.size(); i++) c.insert({keys[i], (V)i});
    std::atomic<long long> find_ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < find_threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) { for (const auto& k : keys) { c.contains(k); local++; } }
            find_ops += local;
        });
    }
    for (int t = 0; t < write_threads; t++) {
        workers.emplace_back([&, t]() {
            int i = 0;
            while (!stop) {
                for (size_t j = 0; j < keys.size(); j++) {
                    if (j % 2 == 0) c.insert({keys[j], (V)(t*10000 + i++)});
                    else c.erase(keys[j]);
                }
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return find_ops * 1000.0 / ms;
}

template<typename Keys>
void run_benchmark(const std::string& name, const Keys& keys, int ms) {
    using namespace gteitelbaum;
    
    std::cout << "## " << name << "\n\n";
    std::cout << "Keys: " << keys.size() << "\n\n";
    
    std::cout << "### FIND\n\n";
    std::cout << "| Threads | tktrie<READ> | tktrie<WRITE> | std::map | std::unordered_map | READ/map | WRITE/map |\n";
    std::cout << "|---------|--------------|---------------|----------|-------------------|----------|----------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        tktrie<std::string, int, Sync::READ> tr;
        tktrie<std::string, int, Sync::WRITE> tw;
        locked_map<std::string, int> lm;
        locked_umap<std::string, int> lu;
        for (size_t i = 0; i < keys.size(); i++) {
            tr.insert({keys[i], (int)i});
            tw.insert({keys[i], (int)i});
            lm.insert({keys[i], (int)i});
            lu.insert({keys[i], (int)i});
        }
        double r = bench_find(tr, keys, threads, ms);
        double w = bench_find(tw, keys, threads, ms);
        double m = bench_find(lm, keys, threads, ms);
        double u = bench_find(lu, keys, threads, ms);
        printf("| %d | %.1fM | %.1fM | %.1fM | %.1fM | %.1fx | %.1fx |\n",
               threads, r/1e6, w/1e6, m/1e6, u/1e6, r/m, w/m);
    }
    
    std::cout << "\n### INSERT\n\n";
    std::cout << "| Threads | tktrie<READ> | tktrie<WRITE> | std::map | std::unordered_map | READ/map | WRITE/map |\n";
    std::cout << "|---------|--------------|---------------|----------|-------------------|----------|----------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        tktrie<std::string, int, Sync::READ> tr;
        tktrie<std::string, int, Sync::WRITE> tw;
        locked_map<std::string, int> lm;
        locked_umap<std::string, int> lu;
        double r = bench_insert<decltype(tr), Keys, int>(tr, keys, threads, ms);
        double w = bench_insert<decltype(tw), Keys, int>(tw, keys, threads, ms);
        double m = bench_insert<decltype(lm), Keys, int>(lm, keys, threads, ms);
        double u = bench_insert<decltype(lu), Keys, int>(lu, keys, threads, ms);
        printf("| %d | %.1fM | %.1fM | %.1fM | %.1fM | %.1fx | %.1fx |\n",
               threads, r/1e6, w/1e6, m/1e6, u/1e6, r/m, w/m);
    }
    
    std::cout << "\n### ERASE\n\n";
    std::cout << "| Threads | tktrie<READ> | tktrie<WRITE> | std::map | std::unordered_map | READ/map | WRITE/map |\n";
    std::cout << "|---------|--------------|---------------|----------|-------------------|----------|----------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        tktrie<std::string, int, Sync::READ> tr;
        tktrie<std::string, int, Sync::WRITE> tw;
        locked_map<std::string, int> lm;
        locked_umap<std::string, int> lu;
        for (size_t i = 0; i < keys.size(); i++) {
            tr.insert({keys[i], (int)i});
            tw.insert({keys[i], (int)i});
            lm.insert({keys[i], (int)i});
            lu.insert({keys[i], (int)i});
        }
        double r = bench_erase(tr, keys, threads, ms);
        double w = bench_erase(tw, keys, threads, ms);
        double m = bench_erase(lm, keys, threads, ms);
        double u = bench_erase(lu, keys, threads, ms);
        printf("| %d | %.1fM | %.1fM | %.1fM | %.1fM | %.1fx | %.1fx |\n",
               threads, r/1e6, w/1e6, m/1e6, u/1e6, r/m, w/m);
    }
    
    std::cout << "\n### FIND with Concurrent Writers\n\n";
    std::cout << "| Readers | Writers | tktrie<READ> | tktrie<WRITE> | std::map | std::unordered_map | READ/map | WRITE/map |\n";
    std::cout << "|---------|---------|--------------|---------------|----------|-------------------|----------|----------|\n";
    
    for (auto [f, wr] : std::vector<std::pair<int,int>>{{4,0}, {4,2}, {4,4}, {8,0}, {8,4}}) {
        double r = bench_mixed_find<tktrie<std::string, int, Sync::READ>, Keys, int>(keys, f, wr, ms);
        double w = bench_mixed_find<tktrie<std::string, int, Sync::WRITE>, Keys, int>(keys, f, wr, ms);
        double m = bench_mixed_find<locked_map<std::string, int>, Keys, int>(keys, f, wr, ms);
        double u = bench_mixed_find<locked_umap<std::string, int>, Keys, int>(keys, f, wr, ms);
        printf("| %d | %d | %.1fM | %.1fM | %.1fM | %.1fM | %.1fx | %.1fx |\n",
               f, wr, r/1e6, w/1e6, m/1e6, u/1e6, r/m, w/m);
    }
    std::cout << "\n";
}

int main() {
    constexpr int MS = 500;
    std::cout << "# tktrie Benchmark: READ vs WRITE Synchronization\n\n";
    std::cout << "- **READ**: RCU-style lock-free reads, global mutex + COW for writes\n";
    std::cout << "- **WRITE**: Per-node spinlocks with hand-over-hand locking\n";
    std::cout << "- Duration: " << MS << "ms per test\n\n";
    run_benchmark("uint64 Keys (10,000 random)", INT_KEYS, MS);
    run_benchmark("String Keys (1,000 words)", STRING_KEYS, MS);
    return 0;
}
