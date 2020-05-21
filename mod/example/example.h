#import <third_party/twltree/twltree.h>

enum messages { UPSERT = 31, DELETE = 42, QUERY = 53};
@interface Example : DefaultExecutor {
@public
	struct twltree_t index;
}
@end
