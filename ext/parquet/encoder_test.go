package parquet_test

import (
	"bytes"

	"github.com/bsm/feedx"
	"github.com/bsm/feedx/ext/parquet"
	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

var _ = Describe("Decoder", func() {
	var subject feedx.FormatEncoder

	BeforeEach(func() {
		var err error
		buf := new(bytes.Buffer)
		format := &parquet.Format{
			EncoderOpts: &parquet.EncoderOpts{
				SchemaDef: `message stat {
					required int64 id;
					required binary city (STRING);
					optional int64 population;
				}`,
			},
		}
		subject, err = format.NewEncoder(buf)
		Expect(err).NotTo(HaveOccurred())
	})

	It("encodes", func() {
		v1 := map[string]interface{}{"id": int64(1), "city": []byte("London"), "population": int64(8982000)}
		Expect(subject.Encode(v1)).To(Succeed())

		v2 := map[string]interface{}{"id": int64(2), "city": []byte("Berlin"), "population": int64(3645000)}
		Expect(subject.Encode(v2)).To(Succeed())

		Expect(subject.Encode("abc")).NotTo(Succeed())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

})
