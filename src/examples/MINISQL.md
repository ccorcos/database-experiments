A relational model / dsl / syntax for creating indexes and working with data.




HERE

- follower timeline, you can't index with sql.
- async database with yields.




```sql
SELECT a.*, b.*
FROM follow AS a
JOIN follow AS b ON a.channel_id = b.channel_id
WHERE a.user_id != b.user_id
AND a.public = true
AND b.public = true
ORDER BY a.user_id, b.user_id;
```

```js
cofollowers = {
	select: { a: "follow", b: "follow" },
	where: {
		and: [
			{ "a.channel_id": { eq: {$: "b.channel_id"} } },
			{ "a.user_id": { neq: {$: "b.user_id"} } },
			{ "a.public": { eq: true }},
			{ "b.public": { eq: true }},
		],
	},
	sort: ["a.user_id", "b.user_id"],
}
```

```
select
a -> follow
b -> follow
where
a.channel_id = b.channel_id
a.user_id != b.user_id
a.public = true
b.public = true
order by
a.user_id b.user_id
```


```js
cofollowers = {
	select: [
		{ follow: { user_id: "userA", channel_id: "channel" } },
		{ follow: { user_id: "userB", channel_id: "channel" } },
	],
	filter: { userA: { neq: {$: "userB"} } },
	sort: ["userA", "userB"],
}
```


```js
cofollowers = {
	select: { a: "follow", b: "follow" },
	where: {
		and: [
			{ "a.channel_id": { eq: {$: "b.channel_id"} } },
			{ "a.user_id": { neq: {$: "b.user_id"} } },
			{ "a.public": { eq: true }},
			{ "b.public": { eq: true }},
		],
	},
	sort: ["a.user_id", "b.user_id"],
}
```

We can make a constaint that
