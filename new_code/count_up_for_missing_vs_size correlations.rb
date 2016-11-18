#within REDDIT main database, within results/missing_results from file available at http://devingaffney.com/files/reddit_missing_data_results.zip
require 'csv'
comments = CSV.read("comments_raw.csv");false
submissions = CSV.read("submissions_raw.csv");false
counted_comments = comments.collect(&:first).counts;false
counted_submissions = submissions.collect(&:first).counts;false
def grab_counts_for_subreddits(subreddits, content_type="post")
  RedditCounter.collection.aggregate([{"$match" => {"content_type" => content_type, "subreddit" => {"$in" => subreddits}}}, {"$group" => {"_id" => "$subreddit", "count" => {"$sum" => "$count"}}}]).collect{|r| [r["_id"], r["count"]]}
end
counted_comments;false
counted_submissions;false
total_comments = {}
total_submissions = {}
counted_submissions.keys.each_slice(100) do |submission_set|
  print "."
  grab_counts_for_subreddits(submission_set, content_type="post").each do |subreddit, count|
    total_submissions[subreddit] = count
  end
end;false
counted_comments.keys.each_slice(100) do |comment_set|
  print "."
  grab_counts_for_subreddits(comment_set, content_type="comment").each do |subreddit, count|
    total_comments[subreddit] = count
  end
end;false

f = CSV.open("missing_data_total_traffic_correlates_submission.csv", "w")
f << ["subreddit", "missing_count", "total_count"]
counted_submissions.each do |subreddit, count|
  f << [subreddit, count, total_submissions[subreddit]]
end;false
f = CSV.open("missing_data_total_traffic_correlates_comment.csv", "w")
f << ["subreddit", "missing_count", "total_count"]
counted_comments.each do |subreddit, count|
  f << [subreddit, count, total_comments[subreddit]]
end;false
rsync home:results/missing_results/missing_data_total_traffic_correlates_submission.csv missing_data_total_traffic_correlates_submission.csv
rsync home:results/missing_results/missing_data_total_traffic_correlates_comment.csv missing_data_total_traffic_correlates_comment.csv