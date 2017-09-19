package stock

import (
	"errors"
	"io/ioutil"
	"net/http"
)

func SendHttpRequest(url, refer string) ([]byte, error) {
	client := &http.Client{}
	Req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	Req.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
	Req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36")
	if len(refer) > 0 {
		Req.Header.Add("Referer", refer)
	}
	Resp, err := client.Do(Req)
	if err != nil {
		return nil, err
	}
	if Resp.StatusCode != http.StatusOK {
		return nil, errors.New(Resp.Status)
	}
	return ioutil.ReadAll(Resp.Body)
}
