#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include "tktrie.h"

const std::vector<std::string> WORDS = {
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
    "does", "did", "got", "may", "part", "find", "long", "down", "made", "many",
    "before", "must", "through", "much", "where", "should", "very", "after", "most", "might",
    "being", "such", "more", "those", "never", "still", "world", "last", "own", "public",
    "while", "next", "less", "both", "life", "under", "same", "right", "here", "state",
    "place", "high", "every", "going", "another", "school", "number", "always", "however", "every",
    "without", "great", "small", "between", "something", "important", "family", "government", "since", "system",
    "group", "always", "children", "often", "money", "called", "water", "business", "almost", "program",
    "point", "hand", "having", "once", "away", "different", "night", "large", "order", "things",
    "already", "nothing", "possible", "second", "rather", "problem", "against", "though", "again", "person",
    "looking", "morning", "house", "during", "side", "power", "further", "young", "turned", "until",
    "something", "start", "given", "working", "anything", "perhaps", "question", "reason", "early", "himself",
    "making", "enough", "better", "open", "show", "case", "seemed", "kind", "name", "read",
    "began", "believe", "several", "across", "office", "later", "usually", "city", "least", "story",
    "coming", "country", "social", "company", "close", "turned", "brought", "national", "service", "idea",
    "although", "behind", "true", "really", "home", "became", "become", "days", "taking", "within",
    "change", "available", "women", "level", "local", "mother", "doing", "development", "certain", "form",
    "whether", "door", "course", "member", "others", "center", "themselves", "best", "short", "white",
    "following", "around", "already", "given", "political", "face", "either", "using", "hours", "together",
    "interest", "whole", "community", "seen", "therefore", "along", "sure", "itself", "experience", "education",
    "keep", "light", "area", "study", "body", "started", "human", "nature", "president", "major",
    "sense", "result", "quite", "toward", "policy", "general", "control", "figure", "action", "process",
    "american", "provide", "based", "free", "support", "include", "believe", "church", "period", "future",
    "room", "common", "effect", "history", "probably", "need", "table", "later", "special", "particular",
    "continue", "personal", "sometimes", "current", "complete", "everything", "actually", "individual", "seems", "care",
    "difficult", "simple", "economic", "research", "perhaps", "clear", "evidence", "recent", "strong", "private",
    "themselves", "remember", "subject", "field", "position", "sense", "cannot", "class", "various", "outside",
    "report", "security", "building", "seems", "meeting", "value", "necessary", "likely", "return", "moment",
    "analysis", "central", "above", "force", "example", "similar", "thus", "stand", "type", "important",
    "society", "entire", "decision", "north", "help", "mind", "everyone", "today", "federal", "terms",
    "view", "international", "according", "finally", "total", "love", "party", "single", "lost", "south",
    "information", "brought", "military", "section", "living", "provides", "main", "student", "role", "available",
    "lines", "director", "knowledge", "court", "expected", "moved", "past", "standard", "expected", "attention",
    "especially", "basic", "half", "appeared", "allow", "treatment", "addition", "chance", "growth", "design",
    "difficult", "itself", "previous", "management", "established", "provides", "wrong", "language", "board", "considered",
    "events", "approach", "actually", "range", "simply", "significant", "situation", "performance", "behavior", "difference",
    "words", "access", "hospital", "issues", "involved", "opportunity", "material", "training", "street", "modern",
    "higher", "blood", "response", "changes", "theory", "population", "inside", "pressure", "financial", "data",
    "effort", "developed", "meaning", "production", "provides", "method", "foreign", "physical", "amount", "traditional",
    "generally", "medical", "patient", "activity", "technology", "voice", "character", "environmental", "natural", "directly",
    "choice", "results", "office", "project", "relationship", "needed", "function", "understanding", "factor", "operation",
    "concerned", "create", "consider", "black", "structure", "positive", "potential", "purpose", "paper", "successful",
    "western", "resource", "prepared", "established", "learning", "serious", "middle", "space", "commission", "successful",
    "running", "model", "condition", "wall", "series", "culture", "official", "congress", "source", "described",
    "increase", "analysis", "created", "science", "organization", "clearly", "network", "surface", "agreement", "agency",
    "works", "practice", "extent", "earlier", "recently", "performance", "additional", "likely", "challenge", "primary",
    "effective", "product", "significant", "presented", "suggested", "technical", "growing", "responsible", "written", "determined",
    "reality", "similar", "developed", "response", "focus", "economy", "final", "professional", "approach", "rules",
    "strategy", "balance", "quality", "legal", "decade", "image", "responsibility", "applied", "critical", "religious",
    "workers", "attention", "movement", "generally", "capital", "associated", "direct", "defined", "values", "appropriate",
    "increase", "effective", "independent", "planning", "regular", "identify", "complex", "commercial", "limited", "demand",
    "energy", "alternative", "original", "conference", "article", "concerned", "application", "principles", "insurance", "procedure",
    "capacity", "statement", "institution", "specific", "benefit", "official", "democratic", "generally", "considerable", "normal",
    "industrial", "standards", "literature", "credit", "pattern", "content", "negative", "aspect", "coverage", "regional",
    "volume", "solutions", "primary", "element", "variable", "communication", "generation", "contract", "customer", "legislation",
    "assessment", "influence", "distinction", "distribution", "executive", "reduction", "selection", "definition", "perspective", "consequence",
    "version", "framework", "revolution", "protection", "resolution", "characteristic", "interpretation", "dimension", "representation", "contribution",
    "recognition", "acquisition", "investigation", "recommendation", "implementation", "consideration", "administration", "participation", "determination", "demonstration",
    "discrimination", "accommodation", "authentication", "authorization", "certification", "classification", "communication", "configuration", "consolidation", "construction",
    "consultation", "consumption", "contamination", "continuation", "contribution", "conversation", "cooperation", "coordination", "corporation", "correlation",
    "customization", "deactivation", "decomposition", "decompression", "decentralization", "declaration", "decommission", "decoration", "dedication", "deformation",
    "degradation", "deliberation", "delineation", "demonstration", "denomination", "departmental", "depreciation", "deprivation", "derivation", "description",
    "desegregation", "desensitization", "designation", "desperation", "destination", "destruction", "deterioration", "determination", "detoxification", "devaluation",
    "devastation", "differentiation", "digitalization", "dimensionality", "disadvantageous", "disappointment", "disassociation", "discontinuation", "discouragement", "discrimination",
    "disenchantment", "disengagement", "disfigurement", "disillusionment", "disintegration", "dismemberment", "disorientation", "displacement", "disproportionate", "disqualification",
    "dissatisfaction", "dissemination", "dissertation", "distillation", "distribution", "diversification", "documentation", "domestication", "dramatization", "duplication",
    "ecclesiastical", "economization", "editorialization", "effectuation", "effortlessness", "egalitarianism", "electrification", "electromagnetic", "electronically", "elementarily",
    "emancipation", "embellishment", "embodiment", "emotionalism", "empowerment", "encapsulation", "encouragement", "endangerment", "enlightenment", "entertainment",
    "enthusiastically", "entrepreneurial", "environmental", "epistemological", "equalization", "equilibrium", "establishment", "evangelization", "evaporation", "exacerbation",
    "exaggeration", "examination", "exasperation", "excommunication", "exemplification", "exhaustiveness", "exhilaration", "existentialism", "experiential", "experimentation",
    "exploitation", "exploration", "exportation", "expropriation", "externalization", "extermination", "extraordinarily", "extraterrestrial", "extravagance", "fabrication",
    "facilitation", "falsification", "familiarization", "fantastically", "fascination", "featherweight", "federalization", "fertilization", "fictionalization", "fingerprinting",
    "firefighting", "flashforward", "flawlessness", "flexibilities", "flourishing", "fluctuation", "fluorescence", "forgetfulness", "formalization", "fortification",
    "fossilization", "fragmentation", "franchising", "fraternization", "freestanding", "friendliness", "frontrunners", "fruitfulness", "frustration", "fulfillment",
    "functionality", "fundamentalism", "fundraising", "galvanization", "generalization", "gentrification", "globalization", "glorification", "governmental", "gracelessness",
    "gradualness", "grandchildren", "grandparents", "gratification", "gravitational", "greenhouse", "groundbreaking", "groundskeeper", "guardianship", "habituation",
    "hallucination", "handcrafted", "handicapping", "handwriting", "happenstance", "hardworking", "headquarters", "heartbreaking", "heartwarming", "helplessness",
    "hemispheric", "hermeneutics", "heterogeneous", "hierarchical", "highlighting", "historically", "homecoming", "homelessness", "homogenization", "hopelessness",
    "horticulture", "hospitality", "housekeeping", "humanitarian", "humiliation", "hybridization", "hydroelectric", "hyperactive", "hypersensitive", "hypothetical",
    "iconoclastic", "idealization", "identification", "ideologically", "idiosyncratic", "illegitimate", "illumination", "illustration", "imaginative", "immeasurable",
    "immobilization", "immortalization", "immunization", "impartiality", "impersonation", "implementation", "implication", "impossibility", "impoverishment", "impressionable",
    "improvisation", "inaccessible", "inadmissible", "inappropriate", "incarceration", "incidentally", "incineration", "incommunicado", "incompatibility", "incomprehensible",
    "inconsistency", "incorporation", "incrimination", "indebtedness", "indefinitely", "independently", "indescribable", "indestructible", "indeterminate", "indifferent",
    "indispensable", "individualism", "individualistic", "individualization", "indoctrination", "industrialism", "industrialization", "ineffectively", "inefficiency", "inevitability",
    "inexperienced", "infinitesimal", "inflammability", "inflexibility", "infrastructure", "ingeniousness", "initialization", "insignificance", "institutionalism", "institutionalization",
    "instrumentation", "insubordinate", "insufficiency", "intellectualism", "intensification", "intentionally", "interactively", "intercontinental", "interdependence", "interdisciplinary",
    "interestingly", "interferometer", "intergovernmental", "intermediary", "intermittently", "internalization", "internationally", "interoperability", "interpretation", "interrelationship"
};

template<typename M>
class guarded_map {
    M data;
    mutable std::shared_mutex mtx;
public:
    bool contains(const std::string& key) const {
        std::shared_lock lock(mtx);
        return data.find(key) != data.end();
    }
    bool insert(const std::pair<std::string, int>& kv) {
        std::unique_lock lock(mtx);
        return data.insert(kv).second;
    }
    bool erase(const std::string& key) {
        std::unique_lock lock(mtx);
        return data.erase(key) > 0;
    }
};

using locked_map = guarded_map<std::map<std::string, int>>;
using locked_umap = guarded_map<std::unordered_map<std::string, int>>;

std::atomic<bool> stop{false};

// Separate benchmarks for each operation type
template<typename Container>
double bench_find(Container& c, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) {
                for (const auto& w : WORDS) { c.contains(w); local++; }
            }
            ops += local;
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container>
double bench_insert(Container& c, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&, t]() {
            long long local = 0;
            int i = 0;
            while (!stop) {
                for (const auto& w : WORDS) { c.insert({w, t*10000 + i++}); local++; }
            }
            ops += local;
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container>
double bench_erase(Container& c, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) {
                for (const auto& w : WORDS) { c.erase(w); local++; }
            }
            ops += local;
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container>
double bench_mixed_find(int find_threads, int write_threads, int ms) {
    Container c;
    for (size_t i = 0; i < WORDS.size(); i++) c.insert({WORDS[i], (int)i});
    
    std::atomic<long long> find_ops{0}, write_ops{0};
    stop = false;
    std::vector<std::thread> workers;
    
    // Find threads
    for (int t = 0; t < find_threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) {
                for (const auto& w : WORDS) { c.contains(w); local++; }
            }
            find_ops += local;
        });
    }
    
    // Write threads (50% insert, 50% erase)
    for (int t = 0; t < write_threads; t++) {
        workers.emplace_back([&, t]() {
            long long local = 0;
            int i = 0;
            while (!stop) {
                for (size_t j = 0; j < WORDS.size(); j++) {
                    if (j % 2 == 0) c.insert({WORDS[j], t*10000 + i++});
                    else c.erase(WORDS[j]);
                    local++;
                }
            }
            write_ops += local;
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return find_ops * 1000.0 / ms;
}

int main() {
    constexpr int MS = 500;
    
    std::cout << "=== Per-Operation Benchmark: RCU Trie vs Guarded map/umap ===\n";
    std::cout << "Words: " << WORDS.size() << "\n\n";
    
    // FIND benchmark
    std::cout << "FIND (contains) - ops/sec:\n";
    std::cout << "Threads |   RCU Trie |   std::map |  std::umap | RCU/map | RCU/umap\n";
    std::cout << "--------|------------|------------|------------|---------|----------\n";
    
    for (int threads : {1, 2, 4, 8}) {
        gteitelbaum::tktrie<std::string, int> rcu_t;
        locked_map lm;
        locked_umap lu;
        for (size_t i = 0; i < WORDS.size(); i++) {
            rcu_t.insert({WORDS[i], (int)i});
            lm.insert({WORDS[i], (int)i});
            lu.insert({WORDS[i], (int)i});
        }
        
        double rcu = bench_find(rcu_t, threads, MS);
        double map = bench_find(lm, threads, MS);
        double umap = bench_find(lu, threads, MS);
        printf("%7d | %10.1fM | %10.1fM | %10.1fM | %7.1fx | %8.1fx\n",
               threads, rcu/1e6, map/1e6, umap/1e6, rcu/map, rcu/umap);
    }
    
    // INSERT benchmark
    std::cout << "\nINSERT - ops/sec:\n";
    std::cout << "Threads |   RCU Trie |   std::map |  std::umap | RCU/map | RCU/umap\n";
    std::cout << "--------|------------|------------|------------|---------|----------\n";
    
    for (int threads : {1, 2, 4, 8}) {
        gteitelbaum::tktrie<std::string, int> rcu_t;
        locked_map lm;
        locked_umap lu;
        
        double rcu = bench_insert(rcu_t, threads, MS);
        double map = bench_insert(lm, threads, MS);
        double umap = bench_insert(lu, threads, MS);
        printf("%7d | %10.1fM | %10.1fM | %10.1fM | %7.1fx | %8.1fx\n",
               threads, rcu/1e6, map/1e6, umap/1e6, rcu/map, rcu/umap);
    }
    
    // ERASE benchmark
    std::cout << "\nERASE - ops/sec:\n";
    std::cout << "Threads |   RCU Trie |   std::map |  std::umap | RCU/map | RCU/umap\n";
    std::cout << "--------|------------|------------|------------|---------|----------\n";
    
    for (int threads : {1, 2, 4, 8}) {
        gteitelbaum::tktrie<std::string, int> rcu_t;
        locked_map lm;
        locked_umap lu;
        for (size_t i = 0; i < WORDS.size(); i++) {
            rcu_t.insert({WORDS[i], (int)i});
            lm.insert({WORDS[i], (int)i});
            lu.insert({WORDS[i], (int)i});
        }
        
        double rcu = bench_erase(rcu_t, threads, MS);
        double map = bench_erase(lm, threads, MS);
        double umap = bench_erase(lu, threads, MS);
        printf("%7d | %10.1fM | %10.1fM | %10.1fM | %7.1fx | %8.1fx\n",
               threads, rcu/1e6, map/1e6, umap/1e6, rcu/map, rcu/umap);
    }
    
    // FIND with concurrent writes
    std::cout << "\nFIND with concurrent WRITERS (insert+erase) - find ops/sec:\n";
    std::cout << "Find/Write |   RCU Trie |   std::map |  std::umap | RCU/map | RCU/umap\n";
    std::cout << "-----------|------------|------------|------------|---------|----------\n";
    
    for (auto [f, w] : std::vector<std::pair<int,int>>{{4,0}, {4,1}, {4,2}, {4,4}, {8,0}, {8,2}, {8,4}}) {
        double rcu = bench_mixed_find<gteitelbaum::tktrie<std::string, int>>(f, w, MS);
        double map = bench_mixed_find<locked_map>(f, w, MS);
        double umap = bench_mixed_find<locked_umap>(f, w, MS);
        printf("    %d / %d | %10.1fM | %10.1fM | %10.1fM | %7.1fx | %8.1fx\n",
               f, w, rcu/1e6, map/1e6, umap/1e6, rcu/map, rcu/umap);
    }
    
    return 0;
}
