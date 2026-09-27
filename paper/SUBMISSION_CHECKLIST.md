# Paper Submission Checklist — nats-lens

**Target**: USENIX ATC 2027 (deadline ~November 2026)  
**Fallback**: SREcon 2027 Americas (deadline ~March 2027)

## Data Status
- [x] 30-round detection coverage: 100% all 5 types
- [x] Detection latency P50/P95/P99 per type
- [ ] False positive test: 1800s run IN PROGRESS (completes ~03:30)
- [ ] Multi-language (Go + Python): IN PROGRESS
- [ ] Overhead CSV: IN PROGRESS (Scenario 7)
- [ ] Scalability CSV: IN PROGRESS

## Paper Sections
- [x] Abstract (with real numbers)
- [x] Introduction
- [x] Background (JetStream model, related work)
- [x] Problem: 5 formal violation definitions + Theorem 1
- [x] System design (all components including init, Apply Now)
- [x] Evaluation 5.1–5.7 (filled with real data)
- [x] Related work
- [x] Conclusion

## Still Needed Before Submission
- [ ] LaTeX formatting (currently in Markdown)
- [ ] Figures referenced as Figure N (currently PDF files)
- [ ] arXiv preprint upload
- [ ] GitHub repo created and code pushed
- [ ] LICENSE file added
- [ ] Author affiliation decision (use Palo Alto Networks or independent?)

## ATC-Specific Requirements
- Page limit: 12 pages (2-column ACM format)
- Artifact evaluation: submit eval harness as artifact
- Anonymous review: blind submission required

## Key Numbers for ATC Submission
- Detection: 5/5 types, 30/30 rounds, 100%
- Latency: P50 2002ms–8013ms (within 3 poll cycles)
- False positives: 0 in 1800s
- Languages: Rust, Go, Python verified
- Overhead: <72 NATS API req/poll for 50 consumers
- Memory: <720KB for 200 consumers
