package collab

import (
	"fmt"
	"strings"
	"sync"
)

// AggregationStrategy 聚合策略
type AggregationStrategy string

const (
	// AggConcat 拼接所有结果
	AggConcat AggregationStrategy = "concat"
	// AggBest 选择最佳结果（按评分）
	AggBest AggregationStrategy = "best"
	// AggVote 投票选择
	AggVote AggregationStrategy = "vote"
	// AggMerge 合并去重
	AggMerge AggregationStrategy = "merge"
	// AggSummary 摘要合并
	AggSummary AggregationStrategy = "summary"
)

// AggregationResult 聚合结果
type AggregationResult struct {
	Strategy  AggregationStrategy `json:"strategy"`
	Output    string              `json:"output"`
	Count     int                 `json:"count"`                // 参与聚合的子结果数
	BestIndex int                 `json:"best_index,omitempty"` // 最佳结果索引（AggBest）
	Votes     map[string]int      `json:"votes,omitempty"`      // 投票结果（AggVote）
	Metadata  map[string]string   `json:"metadata,omitempty"`
}

// Aggregator 结果聚合器
type Aggregator struct {
	mu      sync.RWMutex
	scorers map[AggregationStrategy]ScorerFunc
}

// ScorerFunc 评分函数 — 返回 0~1 的分数
type ScorerFunc func(result string) float64

// NewAggregator 创建聚合器
func NewAggregator() *Aggregator {
	return &Aggregator{
		scorers: make(map[AggregationStrategy]ScorerFunc),
	}
}

// SetScorer installs a caller's quality evaluator. Without one, best preserves
// all candidates rather than pretending output length measures correctness.
func (a *Aggregator) SetScorer(strategy AggregationStrategy, scorer ScorerFunc) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if scorer == nil {
		delete(a.scorers, strategy)
	} else {
		a.scorers[strategy] = scorer
	}
}

// Aggregate 聚合子任务结果
func (a *Aggregator) Aggregate(strategy AggregationStrategy, results []string) *AggregationResult {
	if len(results) == 0 {
		return &AggregationResult{
			Strategy: strategy,
			Output:   "",
			Count:    0,
		}
	}

	// 过滤空结果
	nonEmpty := make([]string, 0, len(results))
	for _, r := range results {
		if strings.TrimSpace(r) != "" {
			nonEmpty = append(nonEmpty, r)
		}
	}

	if len(nonEmpty) == 0 {
		return &AggregationResult{
			Strategy: strategy,
			Output:   "",
			Count:    0,
		}
	}

	switch strategy {
	case AggConcat:
		return a.aggregateConcat(nonEmpty)
	case AggBest:
		return a.aggregateBest(nonEmpty)
	case AggVote:
		return a.aggregateVote(nonEmpty)
	case AggMerge:
		return a.aggregateMerge(nonEmpty)
	case AggSummary:
		return a.aggregateSummary(nonEmpty)
	default:
		return a.aggregateConcat(nonEmpty)
	}
}

// aggregateConcat 拼接
func (a *Aggregator) aggregateConcat(results []string) *AggregationResult {
	return &AggregationResult{
		Strategy: AggConcat,
		Output:   strings.Join(results, "\n\n---\n\n"),
		Count:    len(results),
	}
}

// aggregateBest 选择最佳
func (a *Aggregator) aggregateBest(results []string) *AggregationResult {
	a.mu.RLock()
	scorer, ok := a.scorers[AggBest]
	a.mu.RUnlock()

	if !ok {
		return &AggregationResult{Strategy: AggBest, Output: strings.Join(results, "\n\n---\n\n"), Count: len(results), BestIndex: -1,
			Metadata: map[string]string{"selection": "unscored", "reason": "quality evaluator not configured"}}
	}

	bestIdx := 0
	bestScore := -1.0

	for i, r := range results {
		score := scorer(r)
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	return &AggregationResult{
		Strategy:  AggBest,
		Output:    results[bestIdx],
		Count:     len(results),
		BestIndex: bestIdx,
		Metadata: map[string]string{
			"best_score": fmt.Sprintf("%.4f", bestScore),
		},
	}
}

// aggregateVote counts complete answers; shared prefixes are not consensus.
func (a *Aggregator) aggregateVote(results []string) *AggregationResult {
	groups := make(map[string][]int)
	for i, r := range results {
		prefix := strings.TrimSpace(r)
		groups[prefix] = append(groups[prefix], i)
	}

	// 找最大组
	bestPrefix := ""
	bestCount := 0
	for _, r := range results {
		prefix := strings.TrimSpace(r)
		indices := groups[prefix]
		if len(indices) > bestCount {
			bestCount = len(indices)
			bestPrefix = prefix
		}
	}

	// 构建投票结果
	votes := make(map[string]int)
	for _, indices := range groups {
		votes[fmt.Sprintf("group_%d", indices[0])] = len(indices)
	}

	// 使用最大组的第一个结果
	output := ""
	metadata := map[string]string{"comparison": "complete_answer", "consensus": "false"}
	if indices, ok := groups[bestPrefix]; ok && len(indices) > 0 {
		output = results[indices[0]]
	}
	if bestCount*2 > len(results) {
		metadata["consensus"] = "true"
	} else {
		output = strings.Join(results, "\n\n---\n\n")
	}

	return &AggregationResult{
		Strategy: AggVote,
		Output:   output,
		Count:    len(results),
		Votes:    votes,
		Metadata: metadata,
	}
}

// aggregateMerge 合并去重
func (a *Aggregator) aggregateMerge(results []string) *AggregationResult {
	seen := make(map[string]bool)
	var unique []string

	for _, r := range results {
		// 按行去重
		lines := strings.Split(r, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !seen[line] {
				seen[line] = true
			}
		}
	}

	// 重建去重后的结果
	for _, r := range results {
		lines := strings.Split(r, "\n")
		var merged []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && seen[trimmed] {
				merged = append(merged, line)
				delete(seen, trimmed) // 只保留第一次出现
			}
		}
		if len(merged) > 0 {
			unique = append(unique, strings.Join(merged, "\n"))
		}
	}

	return &AggregationResult{
		Strategy: AggMerge,
		Output:   strings.Join(unique, "\n"),
		Count:    len(results),
		Metadata: map[string]string{
			"unique_lines": fmt.Sprintf("%d", len(seen)+countNonEmpty(unique)),
		},
	}
}

// aggregateSummary 摘要合并 — 每个结果取前 N 字符
func (a *Aggregator) aggregateSummary(results []string) *AggregationResult {
	const maxPerResult = 200

	var parts []string
	for i, r := range results {
		summary := r
		chars := []rune(r)
		if len(chars) > maxPerResult {
			summary = string(chars[:maxPerResult]) + "..."
		}
		parts = append(parts, fmt.Sprintf("[Agent %d] %s", i+1, summary))
	}

	return &AggregationResult{
		Strategy: AggSummary,
		Output:   strings.Join(parts, "\n\n"),
		Count:    len(results),
	}
}

// countNonEmpty 计算非空行数
func countNonEmpty(lines []string) int {
	count := 0
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			count++
		}
	}
	return count
}
