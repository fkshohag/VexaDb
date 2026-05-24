// PDF extraction endpoint for the admin panel.
//
// POST /pdf/extract  (multipart/form-data)
//
//   file              the PDF file (required)
//   chunk_size        optional, characters per chunk        (default 1200)
//   chunk_overlap     optional, characters of overlap       (default 150)
//
// Returns:
//
//	{
//	  "filename": "paper.pdf",
//	  "pages":    14,
//	  "chars":    32145,
//	  "chunks": [
//	    {"idx": 0, "text": "...", "page": 1, "char_start": 0, "char_end": 1200},
//	    ...
//	  ]
//	}
//
// The frontend then embeds each chunk via /embed (existing proxy) and upserts
// into the chosen collection — so this endpoint touches *zero* DB code.
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/labstack/echo/v4"
	"github.com/ledongthuc/pdf"
)

const (
	defaultChunkSize    = 1200
	defaultChunkOverlap = 150
	// Hard upper bound on uploaded PDF size (bytes). Echo's BodyLimit
	// middleware enforces this in main.go too.
	maxPdfBytes = 25 * 1024 * 1024
)

type pdfChunk struct {
	Idx       int    `json:"idx"`
	Text      string `json:"text"`
	Page      int    `json:"page"` // 1-based; first page that contributed text to this chunk
	CharStart int    `json:"char_start"`
	CharEnd   int    `json:"char_end"`
}

type pdfExtractResponse struct {
	Filename string     `json:"filename"`
	Pages    int        `json:"pages"`
	Chars    int        `json:"chars"`
	Chunks   []pdfChunk `json:"chunks"`
}

func handlePdfExtract(c echo.Context) error {
	fh, err := c.FormFile("file")
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "missing 'file' field (multipart/form-data PDF upload)",
		})
	}
	if fh.Size > maxPdfBytes {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("file too large (%d bytes); max %d", fh.Size, maxPdfBytes),
		})
	}

	chunkSize := parseIntForm(c, "chunk_size", defaultChunkSize)
	chunkOverlap := parseIntForm(c, "chunk_overlap", defaultChunkOverlap)
	if chunkSize < 100 {
		chunkSize = 100
	}
	if chunkSize > 8192 {
		chunkSize = 8192
	}
	if chunkOverlap < 0 {
		chunkOverlap = 0
	}
	if chunkOverlap >= chunkSize {
		// Overlap must be smaller than chunk size, otherwise we'd loop forever.
		chunkOverlap = chunkSize / 4
	}

	src, err := fh.Open()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "open uploaded file: " + err.Error(),
		})
	}
	defer src.Close()

	// pdf.NewReader needs an io.ReaderAt + size, so slurp into memory. Cap is
	// already enforced above — at most 25MB.
	buf, err := io.ReadAll(src)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "read uploaded file: " + err.Error(),
		})
	}

	pages, perPage, err := extractPdfText(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, map[string]string{
			"error": "PDF parse failed: " + err.Error(),
		})
	}

	chunks := chunkPagesByChars(perPage, chunkSize, chunkOverlap)

	totalChars := 0
	for _, p := range perPage {
		totalChars += len(p)
	}

	return c.JSON(http.StatusOK, pdfExtractResponse{
		Filename: fh.Filename,
		Pages:    pages,
		Chars:    totalChars,
		Chunks:   chunks,
	})
}

// extractPdfText reads every page and returns (pageCount, perPageText).
// perPageText[i-1] is the text of page i, normalized (whitespace squashed).
// Errors from the underlying parser are wrapped with the page number.
func extractPdfText(r io.ReaderAt, size int64) (int, []string, error) {
	reader, err := pdf.NewReader(r, size)
	if err != nil {
		return 0, nil, err
	}
	pageCount := reader.NumPage()
	perPage := make([]string, pageCount)
	for i := 1; i <= pageCount; i++ {
		page := reader.Page(i)
		if page.V.IsNull() {
			continue
		}
		// `GetPlainText(nil)` swallows fonts we can't decode and returns the
		// rest. The library can still panic on a malformed PDF, so guard with
		// a defer/recover scoped to this page.
		text, perr := safePageText(page)
		if perr != nil {
			return pageCount, perPage, fmt.Errorf("page %d: %w", i, perr)
		}
		perPage[i-1] = normalizeWhitespace(text)
	}
	return pageCount, perPage, nil
}

func safePageText(page pdf.Page) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic decoding page: %v", r)
		}
	}()
	return page.GetPlainText(nil)
}

// chunkPagesByChars walks every page of text and emits fixed-size chunks
// with `overlap` characters carried into the next chunk. Page boundaries
// are respected for the `page` annotation only — chunks are NOT cut at
// page boundaries (a single chunk can span several pages of small text,
// which is what RAG callers actually want).
func chunkPagesByChars(perPage []string, size, overlap int) []pdfChunk {
	type tagged struct {
		ch   rune
		page int
	}
	// Flatten the document into a (rune, page) stream so we can attribute
	// chunk → page at any character offset.
	var stream []tagged
	for i, p := range perPage {
		if p == "" {
			continue
		}
		for _, r := range p {
			stream = append(stream, tagged{ch: r, page: i + 1})
		}
		// Insert a single space at page boundaries so words don't merge.
		if i+1 < len(perPage) {
			stream = append(stream, tagged{ch: ' ', page: i + 1})
		}
	}
	if len(stream) == 0 {
		return nil
	}

	step := size - overlap
	if step <= 0 {
		step = size
	}

	var chunks []pdfChunk
	idx := 0
	for start := 0; start < len(stream); start += step {
		end := start + size
		if end > len(stream) {
			end = len(stream)
		}
		// Snap end back to a word boundary if we're in the middle of one,
		// but never less than 80% of the requested size — avoids tiny tails.
		if end < len(stream) {
			minEnd := start + (size*4)/5
			for end > minEnd && !unicode.IsSpace(stream[end-1].ch) {
				end--
			}
		}
		var sb strings.Builder
		sb.Grow(end - start)
		for j := start; j < end; j++ {
			sb.WriteRune(stream[j].ch)
		}
		text := strings.TrimSpace(sb.String())
		if text == "" {
			continue
		}
		chunks = append(chunks, pdfChunk{
			Idx:       idx,
			Text:      text,
			Page:      stream[start].page,
			CharStart: start,
			CharEnd:   end,
		})
		idx++
		if end == len(stream) {
			break
		}
	}
	return chunks
}

// normalizeWhitespace collapses runs of whitespace (including the literal
// CR/LF/TAB and the U+00A0 non-breaking space PDFs love to emit) into a
// single ASCII space. Preserves a single newline between paragraphs by
// keeping a sentinel that we then map to space — most embedders don't care
// about layout, and chunking is char-based.
func normalizeWhitespace(s string) string {
	if s == "" {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) || r == 0xA0 {
			if !prevSpace {
				sb.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		sb.WriteRune(r)
		prevSpace = false
	}
	return strings.TrimSpace(sb.String())
}

func parseIntForm(c echo.Context, key string, def int) int {
	v := strings.TrimSpace(c.FormValue(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
