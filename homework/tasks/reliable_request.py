import abc
import asyncio
import httpx
import logging


class ResultsObserver(abc.ABC):
    @abc.abstractmethod
    def observe(self, data: bytes) -> None:
        pass


async def do_reliable_request(url: str, observer: ResultsObserver) -> None:
    """
    Одна из главных проблем распределённых систем - это ненадёжность связи.

    Ваша задача заключается в том, чтобы таким образом исправить этот код, чтобы он
    умел переживать возвраты ошибок и таймауты со стороны сервера, гарантируя
    успешный запрос (в реальной жизни такая гарантия невозможна, но мы чуть упростим себе задачу).

    Все успешно полученные результаты должны регистрироваться с помощью обсёрвера.
    """

    max_retries = 5
    retry_delay = 1

    async with httpx.AsyncClient() as client:
        for _ in range(max_retries):
            try:
                response = await client.get(url,
                                            timeout=5.0)
                response.raise_for_status()
                data = response.content
                observer.observe(data)
                return
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                logging.error(f"Ошибка запроса: {e}")
                await asyncio.sleep(retry_delay)
        raise Exception(
            "Failed to make a reliable request after multiple attempts.")
