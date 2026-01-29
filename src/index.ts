import html from '../index.html';
import { gzipSync } from 'fflate';

interface LogEntry {
	timestamp: string;
	[key: string]: any;
}

interface AnalyticsEngineResponse {
	rows: number;
	rows_before_limit_at_least: number;
	data: LogEntry[];
}

async function fetchLogsByDate(env: Env, targetDate?: string): Promise<LogEntry[]> {
	let date: Date;

	if (targetDate) {
		if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
			throw new Error('Invalid date format. Use YYYY-MM-DD');
		}
		date = new Date(targetDate + 'T00:00:00Z');
		if (isNaN(date.getTime())) {
			throw new Error('Invalid date provided');
		}

		const oneMonthAgo = new Date();
		oneMonthAgo.setUTCMonth(oneMonthAgo.getUTCMonth() - 1);
		oneMonthAgo.setUTCHours(0, 0, 0, 0);

		if (date < oneMonthAgo) {
			throw new Error(`Date ${targetDate} is too old. Logs older than one month may have expired and are not available.`);
		}

		const today = new Date();
		today.setUTCHours(23, 59, 59, 999);

		if (date > today) {
			throw new Error(`Date ${targetDate} is in the future. Cannot collect future logs.`);
		}
	} else {
		date = new Date();
		date.setUTCDate(date.getUTCDate() - 1);
	}

	const startDate = new Date(date);
	startDate.setUTCHours(0, 0, 0, 0);

	const endDate = new Date(date);
	endDate.setUTCHours(23, 59, 59, 999);

	const startDateStr = startDate.toISOString().replace('T', ' ').split('.')[0];
	const endDateStr = endDate.toISOString().replace('T', ' ').split('.')[0];

	const query = `SELECT * FROM byrdocs WHERE timestamp >= toDateTime('${startDateStr}') AND timestamp < toDateTime('${endDateStr}')`;
	console.log('SQL Query:', query);

	const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/analytics_engine/sql`, {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
		},
		body: query,
	});

	if (!response.ok) {
		throw new Error(`Analytics Engine API failed (${response.status} ${response.statusText}): ${await response.text()}`);
	}

	const data: AnalyticsEngineResponse = await response.json();
	return data.data || [];
}

async function saveLogsToR2(env: Env, logs: LogEntry[], date: string): Promise<void> {
	const fileName = `${date}.json.gz`;
	const logsJson = JSON.stringify(logs);
	const logsBuffer = new TextEncoder().encode(logsJson);
	const logsGzip = gzipSync(logsBuffer);

	await env.R2_BUCKET.put(fileName, logsGzip, {
		httpMetadata: {
			contentType: 'application/gzip',
		},
	});

	console.log(`Saved ${logs.length} log entries to R2: ${fileName}`);
}

async function processLogs(env: Env, targetDate?: string): Promise<{ date: string; count: number }> {
	try {
		const logs = await fetchLogsByDate(env, targetDate);

		let dateStr: string;
		if (targetDate) {
			dateStr = targetDate;
		} else {
			const yesterday = new Date();
			yesterday.setUTCDate(yesterday.getUTCDate() - 1);
			dateStr = yesterday.toISOString().split('T')[0];
		}

		await saveLogsToR2(env, logs, dateStr);

		return { date: dateStr, count: logs.length };
	} catch (error) {
		console.error('Failed to process logs:', error);
		throw error;
	}
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === '/trigger-logs' && request.method === 'POST') {
			const authHeader = request.headers.get('Authorization');

			if (!authHeader || authHeader !== `Bearer ${env.CLOUDFLARE_API_TOKEN}`) {
				return new Response('Unauthorized', { status: 401 });
			}

			try {
				const dateParam = url.searchParams.get('date');
				const result = await processLogs(env, dateParam || undefined);

				return new Response(
					JSON.stringify({
						success: true,
						message: `Processed logs for ${result.date}`,
						date: result.date,
						count: result.count,
					}),
					{
						headers: { 'Content-Type': 'application/json' },
					}
				);
			} catch (error) {
				return new Response(
					JSON.stringify({
						success: false,
						error: error instanceof Error ? error.message : 'Unknown error',
					}),
					{
						status: 500,
						headers: { 'Content-Type': 'application/json' },
					}
				);
			}
		}

		if (url.pathname === '/') {
			const readme = await cachedFetch("https://raw.githubusercontent.com/byrdocs/byrdocs-logs/refs/heads/main/README.md")
			const htmlWithContent = html.replace(
				'<div id="content"></div>',
				`<div id="content"></div>
				<script>
					const markdownContent = ${JSON.stringify(await readme.text())};
					document.getElementById('content').innerHTML = marked.parse(markdownContent);
				</script>`
			);

			return new Response(htmlWithContent, {
				headers: { 'Content-Type': 'text/html' },
			});
		}

		return new Response('Not Found', { status: 404 });
	},

	async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		console.log(`Scheduled task fired at ${controller.cron}: ${new Date().toISOString()}`);

		try {
			const result = await processLogs(env);
			console.log(`Successfully processed logs for ${result.date}, count: ${result.count}`);
		} catch (error) {
			console.error('Scheduled task failed:', error);
			throw error;
		}
	},
} satisfies ExportedHandler<Env>;


async function cachedFetch(url: string) {
	const cache = await caches.open('byrdocs-logs');
	const hit = await cache.match(url);
	if (hit) return hit;
	const response = await fetch(url);
	await cache.put(url, response.clone());
	return response;
}