package download

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

func clearSecretValidators(meta *Metadata, secret string) {
	if secret == "" {
		return
	}
	replacer := credentialReplacer(secret)
	for _, validator := range []*string{&meta.ETag, &meta.LastModified, &meta.ByteExactETag} {
		if replacer.Replace(*validator) != *validator {
			*validator = ""
		}
	}
}

func redactDownloadError(err error, secret string) error {
	var requestError *url.Error
	if errors.As(err, &requestError) {
		// Avoid including either the original URL or the signed redirect URL.
		err = fmt.Errorf("download: request failed: %w", requestError.Err)
	}
	message := err.Error()
	redacted := credentialReplacer(secret).Replace(message)
	if redacted != message {
		return errors.New(redacted)
	}
	return err
}

func credentialReplacer(secret string) *strings.Replacer {
	const redacted = "[redacted]"
	return strings.NewReplacer(secret, redacted,
		url.QueryEscape(secret), redacted, url.PathEscape(secret), redacted)
}
