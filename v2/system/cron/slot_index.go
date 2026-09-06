package cron

type slotIndex struct {
	slots         map[int64]map[string]struct{}
	nextSlotByJob map[string]int64
}

func newSlotIndex() *slotIndex {
	return &slotIndex{
		slots:         make(map[int64]map[string]struct{}),
		nextSlotByJob: make(map[string]int64),
	}
}

func (s *slotIndex) Put(jobID string, slot int64) {
	if previous, ok := s.nextSlotByJob[jobID]; ok {
		s.Remove(jobID, previous)
	}
	bucket := s.slots[slot]
	if bucket == nil {
		bucket = make(map[string]struct{})
		s.slots[slot] = bucket
	}
	bucket[jobID] = struct{}{}
	s.nextSlotByJob[jobID] = slot
}

func (s *slotIndex) Remove(jobID string, slot int64) {
	bucket := s.slots[slot]
	if bucket == nil {
		return
	}
	delete(bucket, jobID)
	if len(bucket) == 0 {
		delete(s.slots, slot)
	}
	delete(s.nextSlotByJob, jobID)
}

func (s *slotIndex) RemoveJob(jobID string) {
	if slot, ok := s.nextSlotByJob[jobID]; ok {
		s.Remove(jobID, slot)
	}
}

func (s *slotIndex) Take(slot int64) []string {
	bucket := s.slots[slot]
	if bucket == nil {
		return nil
	}
	delete(s.slots, slot)
	ids := make([]string, 0, len(bucket))
	for jobID := range bucket {
		delete(s.nextSlotByJob, jobID)
		ids = append(ids, jobID)
	}
	return ids
}
