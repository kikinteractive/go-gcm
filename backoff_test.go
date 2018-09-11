package fcm

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backoff Provider", func() {
	Context("initializing", func() {
		It("should init successfully", func() {
			p := newExponentialBackoff()
			Expect(p).To(BeAssignableToTypeOf(&exponentialBackoff{}))
			bp := p.(*exponentialBackoff)
			Expect(bp.b).NotTo(BeZero())
			Expect(bp.b.Min).To(Equal(DefaultMinBackoff))
			Expect(bp.b.Max).To(Equal(DefaultMaxBackoff))
			Expect(bp.b.Jitter).To(BeTrue())
			Expect(bp.currentDelay).To(Equal(time.Second))
		})
	})

	Context("initialized", func() {
		var (
			b  backoffProvider
			bp *exponentialBackoff
		)

		BeforeEach(func() {
			b = newExponentialBackoff()
			bp = b.(*exponentialBackoff)
		})

		It("should get minimum duration", func() {
			Expect(b.getMin()).To(Equal(b.(*exponentialBackoff).b.Min))
		})

		It("should decrease minimum duration", func() {
			delay := 100 * time.Millisecond
			b.setMin(delay)
			Expect(bp.b.Min).To(Equal(delay))
		})

		It("should increase minimum duration", func() {
			delay := 2 * time.Second
			b.setMin(delay)
			Expect(bp.b.Min).To(Equal(delay))
			Expect(bp.currentDelay).To(Equal(delay))
		})

		It("should send another", func() {
			s := b.sendAnother()
			Expect(s).To(BeTrue())
		})

		It("should not send another", func() {
			bp.currentDelay = DefaultMaxBackoff + time.Millisecond
			s := b.sendAnother()
			Expect(s).To(BeFalse())
		})

		It("should wait", func() {
			bp.currentDelay = time.Millisecond
			b.wait()
			Expect(bp.currentDelay).NotTo(Equal(time.Millisecond))
		})
	})
})
