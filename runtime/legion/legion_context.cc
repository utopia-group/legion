/* Copyright 2021 Stanford University, NVIDIA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "legion/runtime.h"
#include "legion/legion_tasks.h"
#include "legion/legion_trace.h"
#include "legion/legion_context.h"
#include "legion/legion_instances.h"
#include "legion/legion_views.h"
#include "legion/legion_replication.h"

#define SWAP_PART_KINDS(k1, k2) \
  {                             \
    PartitionKind temp = k1;    \
    k1 = k2;                    \
    k2 = temp;                  \
  }

namespace Legion {
  namespace Internal {

    LEGION_EXTERN_LOGGER_DECLARATIONS

    /////////////////////////////////////////////////////////////
    // Task Context 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    TaskContext::TaskContext(Runtime *rt, SingleTask *owner, int d,
                             const std::vector<RegionRequirement> &reqs,
                             const std::vector<RegionRequirement> &out_reqs,
                             bool inline_t)
      : runtime(rt), owner_task(owner), regions(reqs),
        output_reqs(out_reqs), depth(d),
        next_created_index(reqs.size()),executing_processor(Processor::NO_PROC),
        total_tunable_count(0), overhead_tracker(NULL), task_executed(false),
        has_inline_accessor(false), mutable_priority(false),
        children_complete_invoked(false), children_commit_invoked(false),
        inline_task(inline_t)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    TaskContext::TaskContext(const TaskContext &rhs)
      : runtime(NULL), owner_task(NULL), regions(rhs.regions),
        output_reqs(rhs.output_reqs), depth(-1), inline_task(false)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    TaskContext::~TaskContext(void)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(deletion_counts.empty());
#endif
      // Clean up any local variables that we have
      if (!task_local_variables.empty())
      {
        for (std::map<LocalVariableID,
                      std::pair<void*,void (*)(void*)> >::iterator it = 
              task_local_variables.begin(); it != 
              task_local_variables.end(); it++)
        {
          if (it->second.second != NULL)
            (*it->second.second)(it->second.first);
        }
      }
      if (overhead_tracker != NULL)
        delete overhead_tracker;
    }

    //--------------------------------------------------------------------------
    TaskContext& TaskContext::operator=(const TaskContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    UniqueID TaskContext::get_context_uid(void) const
    //--------------------------------------------------------------------------
    {
      return owner_task->get_unique_op_id();
    }

    //--------------------------------------------------------------------------
    Task* TaskContext::get_task(void)
    //--------------------------------------------------------------------------
    {
      return owner_task;
    }

    //--------------------------------------------------------------------------
    InnerContext* TaskContext::find_parent_context(void)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(owner_task != NULL);
#endif
      return owner_task->get_context();
    }

    //--------------------------------------------------------------------------
    bool TaskContext::is_leaf_context(void) const
    //--------------------------------------------------------------------------
    {
      return false;
    }

    //--------------------------------------------------------------------------
    bool TaskContext::is_inner_context(void) const
    //--------------------------------------------------------------------------
    {
      return false;
    }

#ifdef LEGION_USE_LIBDL
    //--------------------------------------------------------------------------
    void TaskContext::perform_global_registration_callbacks(
                     Realm::DSOReferenceImplementation *dso, RtEvent local_done,
                     RtEvent global_done, std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      // Send messages to all the other nodes to perform it
      for (AddressSpaceID space = 0; 
            space < runtime->total_address_spaces; space++)
      {
        if (space == runtime->address_space)
          continue;
        runtime->send_registration_callback(space, dso, global_done,
                                            preconditions);
      }
    }
#endif

    //--------------------------------------------------------------------------
    void TaskContext::print_once(FILE *f, const char *message) const
    //--------------------------------------------------------------------------
    {
      fprintf(f, "%s", message);
    }

    //--------------------------------------------------------------------------
    void TaskContext::log_once(Realm::LoggerMessage &message) const
    //--------------------------------------------------------------------------
    {
      // Do nothing, just don't deactivate it
    }

    //--------------------------------------------------------------------------
    Future TaskContext::from_value(const void *value, size_t size, bool owned, 
                                   Memory::Kind kind,void (*func)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
      // Set the future result
      RtEvent done;
      FutureInstance *instance = NULL;
      if (size > 0)
      {
        Memory memory = runtime->find_local_memory(executing_processor, kind);
        if (owned)
          instance = new FutureInstance(value, size, memory,
              ApEvent::NO_AP_EVENT, runtime, false/*eager*/,
              true/*external allocation*/, true/*own allocation*/,
              PhysicalInstance::NO_INST, func, executing_processor);
        else
          instance = copy_to_future_inst(value, size, memory, done);
      }
      result.impl->set_result(instance);
      if (done.exists() && !done.has_triggered())
        done.wait();
      return result;
    }

    //--------------------------------------------------------------------------
    ShardID TaskContext::get_shard_id(void) const
    //--------------------------------------------------------------------------
    {
      return 0;
    }

    //--------------------------------------------------------------------------
    size_t TaskContext::get_num_shards(void) const
    //--------------------------------------------------------------------------
    {
      return 1;
    }

    //--------------------------------------------------------------------------
    Future TaskContext::consensus_match(const void *input, void *output,
                                        size_t num_elements,size_t element_size)
    //--------------------------------------------------------------------------
    {
      // No need to do a match here, there is just one shard
      memcpy(output, input, num_elements * element_size);
      Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
      result.impl->set_local(&num_elements, sizeof(num_elements));
      return result;
    }

    //--------------------------------------------------------------------------
    VariantID TaskContext::register_variant(
            const TaskVariantRegistrar &registrar, const void *user_data,
            size_t user_data_size, const CodeDescriptor &desc, bool ret,
            VariantID vid, bool check_task_id)
    //--------------------------------------------------------------------------
    {
      return runtime->register_variant(registrar, user_data, user_data_size,
          desc, ret, vid, check_task_id, false/*check context*/);
    }

    //--------------------------------------------------------------------------
    TraceID TaskContext::generate_dynamic_trace_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_trace_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    MapperID TaskContext::generate_dynamic_mapper_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_mapper_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    ProjectionID TaskContext::generate_dynamic_projection_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_projection_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    ShardingID TaskContext::generate_dynamic_sharding_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_sharding_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    TaskID TaskContext::generate_dynamic_task_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_task_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    ReductionOpID TaskContext::generate_dynamic_reduction_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_reduction_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    CustomSerdezID TaskContext::generate_dynamic_serdez_id(void)
    //--------------------------------------------------------------------------
    {
      return runtime->generate_dynamic_serdez_id(false/*check context*/);
    }

    //--------------------------------------------------------------------------
    bool TaskContext::perform_semantic_attach(bool &global)
    //--------------------------------------------------------------------------
    {
      return true;
    }

    //--------------------------------------------------------------------------
    void TaskContext::post_semantic_attach(void)
    //--------------------------------------------------------------------------
    {
      // Nothing to do here
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::create_index_space(const Domain &bounds, 
                                               TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      return create_index_space_internal(&bounds, type_tag);
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::create_index_space(
                                         const std::vector<DomainPoint> &points)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      switch (points[0].get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            std::vector<Realm::Point<DIM,coord_t> > \
              realm_points(points.size()); \
            for (unsigned idx = 0; idx < points.size(); idx++) \
              realm_points[idx] = Point<DIM,coord_t>(points[idx]); \
            const DomainT<DIM,coord_t> realm_is( \
                (Realm::IndexSpace<DIM,coord_t>(realm_points))); \
            const Domain bounds(realm_is); \
            return create_index_space_internal(&bounds, \
                NT_TemplateHelper::encode_tag<DIM,coord_t>()); \
          }
        LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::create_index_space(const std::vector<Domain> &rects)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      switch (rects[0].get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            std::vector<Realm::Rect<DIM,coord_t> > realm_rects(rects.size()); \
            for (unsigned idx = 0; idx < rects.size(); idx++) \
              realm_rects[idx] = Rect<DIM,coord_t>(rects[idx]); \
            const DomainT<DIM,coord_t> realm_is( \
                (Realm::IndexSpace<DIM,coord_t>(realm_rects))); \
            const Domain bounds(realm_is); \
            return create_index_space_internal(&bounds, \
                NT_TemplateHelper::encode_tag<DIM,coord_t>()); \
          }
        LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::create_index_space_internal(const Domain *bounds,
                                                        TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      IndexSpace handle(runtime->get_unique_index_space_id(),
                        runtime->get_unique_index_tree_id(), type_tag);
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space %x in task%s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id()); 
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_index_space(handle.id);
      runtime->forest->create_index_space(handle, bounds, did); 
      register_index_space_creation(handle);
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::create_unbound_index_space(TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      return create_index_space_internal(NULL, type_tag); 
    }

    //--------------------------------------------------------------------------
    void TaskContext::create_shared_ownership(IndexSpace handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      // Check to see if this is a top-level index space, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_index_space(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_SHARED_OWNERSHIP,
            "Illegal call to create shared ownership for index space %x in " 
            "task %s (UID %lld) which is not a top-level index space. Legion "
            "only permits top-level index spaces to have shared ownership.", 
            handle.get_id(), get_task_name(), get_unique_id())
      runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<IndexSpace,unsigned>::iterator finder = 
        created_index_spaces.find(handle);
      if (finder != created_index_spaces.end())
        finder->second++;
      else
        created_index_spaces[handle] = 1;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::union_index_spaces(
                                          const std::vector<IndexSpace> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (spaces.empty())
        return IndexSpace::NO_SPACE;
      bool none_exists = true;
      for (std::vector<IndexSpace>::const_iterator it = 
            spaces.begin(); it != spaces.end(); it++)
      {
        if (none_exists && it->exists())
          none_exists = false;
        if (spaces[0].get_type_tag() != it->get_type_tag())
          REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'union_index_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      }
      if (none_exists)
        return IndexSpace::NO_SPACE;
      const IndexSpace handle(runtime->get_unique_index_space_id(),
          runtime->get_unique_index_tree_id(), spaces[0].get_type_tag());
      const DistributedID did = runtime->get_available_distributed_id();
      runtime->forest->create_union_space(handle, did, spaces);
      register_index_space_creation(handle);
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_index_space(handle.get_id());
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::intersect_index_spaces(
                                          const std::vector<IndexSpace> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (spaces.empty())
        return IndexSpace::NO_SPACE;
      bool none_exists = true;
      for (std::vector<IndexSpace>::const_iterator it = 
            spaces.begin(); it != spaces.end(); it++)
      {
        if (none_exists && it->exists())
          none_exists = false;
        if (spaces[0].get_type_tag() != it->get_type_tag())
          REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'intersect_index_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      }
      if (none_exists)
        return IndexSpace::NO_SPACE;
      const IndexSpace handle(runtime->get_unique_index_space_id(),
          runtime->get_unique_index_tree_id(), spaces[0].get_type_tag());
      const DistributedID did = runtime->get_available_distributed_id();
      runtime->forest->create_intersection_space(handle, did, spaces);
      register_index_space_creation(handle);
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_index_space(handle.get_id());
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::subtract_index_spaces(
                                              IndexSpace left, IndexSpace right)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (!left.exists())
        return IndexSpace::NO_SPACE;
      if (right.exists() && left.get_type_tag() != right.get_type_tag())
        REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'create_difference_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      const IndexSpace handle(runtime->get_unique_index_space_id(),
          runtime->get_unique_index_tree_id(), left.get_type_tag());
      const DistributedID did = runtime->get_available_distributed_id();
      runtime->forest->create_difference_space(handle, did, left, right);
      register_index_space_creation(handle);
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_index_space(handle.get_id());
      return handle;
    }

    //--------------------------------------------------------------------------
    void TaskContext::create_shared_ownership(IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<IndexPartition,unsigned>::iterator finder = 
        created_index_partitions.find(handle);
      if (finder != created_index_partitions.end())
        finder->second++;
      else
        created_index_partitions[handle] = 1;
    }

    //--------------------------------------------------------------------------
    FieldSpace TaskContext::create_field_space(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      FieldSpace space(runtime->get_unique_field_space_id());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_field.debug("Creating field space %x in task %s (ID %lld)", 
                      space.id, get_task_name(), get_unique_id());
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_field_space(space.id);

      runtime->forest->create_field_space(space, did);
      register_field_space_creation(space);
      return space;
    }

    //--------------------------------------------------------------------------
    FieldSpace TaskContext::create_field_space(
                                         const std::vector<size_t> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      FieldSpace space(runtime->get_unique_field_space_id());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_field.debug("Creating field space %x in task %s (ID %lld)", 
                      space.id, get_task_name(), get_unique_id());
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_field_space(space.id);

      FieldSpaceNode *node = runtime->forest->create_field_space(space, did);
      register_field_space_creation(space);
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
          resulting_fields[idx] = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_field_creation(space.id, 
                                        resulting_fields[idx], sizes[idx]);
      }
      node->initialize_fields(sizes, resulting_fields, serdez_id);
      register_all_field_creations(space, false/*local*/, resulting_fields);
      return space;
    }

    //--------------------------------------------------------------------------
    void TaskContext::create_shared_ownership(FieldSpace handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<FieldSpace,unsigned>::iterator finder = 
        created_field_spaces.find(handle);
      if (finder != created_field_spaces.end())
        finder->second++;
      else
        created_field_spaces[handle] = 1;
    }

    //--------------------------------------------------------------------------
    FieldAllocatorImpl* TaskContext::create_field_allocator(FieldSpace handle,
                                                            bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator finder = 
          field_allocators.find(handle);
        if (finder != field_allocators.end())
          return finder->second;
      }
      // Didn't find it, so have to make, retake the lock in exclusive mode
      FieldSpaceNode *node = runtime->forest->get_node(handle);
      AutoLock priv_lock(privilege_lock);
      // Check to see if we lost the race
      std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator finder = 
        field_allocators.find(handle);
      if (finder != field_allocators.end())
        return finder->second;
      // Don't have one so make a new one
      const RtEvent ready = node->create_allocator(runtime->address_space);
      FieldAllocatorImpl *result = new FieldAllocatorImpl(node, this, ready);
      // Save it for later
      field_allocators[handle] = result;
      return result;
    }

    //--------------------------------------------------------------------------
    void TaskContext::destroy_field_allocator(FieldSpaceNode *node)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      const RtEvent ready = node->destroy_allocator(runtime->address_space);
      if (ready.exists() && !ready.has_triggered())
        ready.wait();
      AutoLock priv_lock(privilege_lock);
      std::map<FieldSpace,FieldAllocatorImpl*>::iterator finder = 
        field_allocators.find(node->handle);
#ifdef DEBUG_LEGION
      assert(finder != field_allocators.end());
#endif
      field_allocators.erase(finder);
    }

    //--------------------------------------------------------------------------
    FieldID TaskContext::allocate_field(FieldSpace space, size_t field_size,
                                        FieldID fid, bool local,
                                        CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (fid == LEGION_AUTO_GENERATE_ID)
        fid = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
      else if (fid >= LEGION_MAX_APPLICATION_FIELD_ID)
        REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
           "Task %s (ID %lld) attempted to allocate a field with ID %d which "
           "exceeds the LEGION_MAX_APPLICATION_FIELD_ID bound set in "
           "legion_config.h", get_task_name(), get_unique_id(), fid)
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_field_creation(space.id, fid, field_size);

      std::set<RtEvent> done_events;
      if (!local)
      {
        const RtEvent precondition = 
          runtime->forest->allocate_field(space, field_size, fid, serdez_id);
        if (precondition.exists())
          done_events.insert(precondition);
      }
      else
        allocate_local_field(space, field_size, fid, 
                             serdez_id, done_events);
      register_field_creation(space, fid, local);
      if (!done_events.empty())
      {
        const RtEvent precondition = Runtime::merge_events(done_events);
        if (precondition.exists() && !precondition.has_triggered())
        {
          if (is_inner_context())
          {
            InnerContext *ctx = static_cast<InnerContext*>(this);
            // Need a fence to make sure that no one tries to use these
            // fields where they haven't been made visible yet
            CreationOp *creator = runtime->get_available_creation_op();
            creator->initialize_fence(ctx, precondition);
            add_to_dependence_queue(creator);
          }
          else
            precondition.wait();
        }
      }
      return fid;
    }

    //--------------------------------------------------------------------------
    void TaskContext::allocate_fields(FieldSpace space,
                                      const std::vector<size_t> &sizes,
                                      std::vector<FieldID> &resulting_fields,
                                      bool local, CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
          resulting_fields[idx] = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_field_creation(space.id, 
                                        resulting_fields[idx], sizes[idx]);
      }
      std::set<RtEvent> done_events;
      if (!local)
      {
        const RtEvent precondition = runtime->forest->allocate_fields(space, 
                                        sizes,  resulting_fields, serdez_id);
        if (precondition.exists())
          done_events.insert(precondition);
      }
      else
        allocate_local_fields(space, sizes, resulting_fields,
                              serdez_id, done_events);
      register_all_field_creations(space, local, resulting_fields);
      if (!done_events.empty())
      {
        const RtEvent precondition = Runtime::merge_events(done_events);
        if (precondition.exists() && !precondition.has_triggered())
        {
          if (is_inner_context())
          {
            // Need a fence to make sure that no one tries to use these
            // fields where they haven't been made visible yet
            InnerContext *ctx = static_cast<InnerContext*>(this);
            CreationOp *creator = runtime->get_available_creation_op();
            creator->initialize_fence(ctx, precondition);
            add_to_dependence_queue(creator);
          }
          else
            precondition.wait();
        }
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::create_shared_ownership(LogicalRegion handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      if (!runtime->forest->is_top_level_region(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_SHARED_OWNERSHIP,
            "Illegal call to create shared ownership for logical region "
            "(%x,%x,%x in task %s (UID %lld) which is not a top-level logical "
            "region. Legion only permits top-level logical regions to have "
            "shared ownerships.", handle.index_space.id, handle.field_space.id,
            handle.tree_id, get_task_name(), get_unique_id())
      runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<LogicalRegion,unsigned>::iterator finder = 
        created_regions.find(handle);
      if (finder != created_regions.end())
        finder->second++;
      else
        created_regions[handle] = 1;
    } 

    //--------------------------------------------------------------------------
    void TaskContext::add_output_region(const OutputRequirement &req,
                                        InstanceSet instances,
                                        bool global,
                                        bool valid)
    //--------------------------------------------------------------------------
    {
      size_t index = output_regions.size();
      OutputRegionImpl *impl = new OutputRegionImpl(index,
                                                    req,
                                                    instances,
                                                    this,
                                                    runtime,
                                                    global,
                                                    valid);
      output_regions.push_back(OutputRegion(impl));
    }

    //--------------------------------------------------------------------------
    PhysicalRegion TaskContext::get_physical_region(unsigned idx)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(idx < regions.size()); // should be one of our original regions
#endif
      return physical_regions[idx];
    } 

    //--------------------------------------------------------------------------
    OutputRegion TaskContext::get_output_region(unsigned idx) const
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(idx < output_regions.size()); //should be one of our output regions
#endif
      return output_regions[idx];
    }

    //--------------------------------------------------------------------------
    void TaskContext::finalize_output_regions(void)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < output_regions.size(); ++idx)
      {
        // Check if the task returned data for all the output fields
        // before we fianlize this output region.
        OutputRegion &output_region = output_regions[idx];
        FieldID unbound_field = 0;
        if (!output_region.impl->is_complete(unbound_field))
        {
          REPORT_LEGION_ERROR(ERROR_UNBOUND_OUTPUT_REGION,
            "Task %s (UID %lld) did not return any instance for field %d "
            "of output requirement %u",
            owner_task->get_task_name(), owner_task->get_unique_id(),
            unbound_field, idx);
        }
        output_region.impl->finalize();
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::destroy_user_lock(Reservation r)
    //--------------------------------------------------------------------------
    {
      // Can only be called from user land so no
      // need to hold the lock
      context_locks.push_back(r);
    }

    //--------------------------------------------------------------------------
    void TaskContext::destroy_user_barrier(ApBarrier b)
    //--------------------------------------------------------------------------
    {
      // Can only be called from user land so no 
      // need to hold the lock
      context_barriers.push_back(b);
    } 

    //--------------------------------------------------------------------------
    void TaskContext::add_created_region(LogicalRegion handle, 
                                const bool task_local, const bool output_region)
    //--------------------------------------------------------------------------
    {
      // Already hold the lock from the caller
      RegionRequirement new_req(handle, LEGION_READ_WRITE, 
                                LEGION_EXCLUSIVE, handle);
      if (output_region)
        new_req.flags |= LEGION_CREATED_OUTPUT_REQUIREMENT_FLAG;
      if (runtime->legion_spy_enabled)
        TaskOp::log_requirement(get_unique_id(), next_created_index, new_req);
      // Put a region requirement with no fields in the list of
      // created requirements, we know we can add any fields for
      // this field space in the future since we own all privileges
      created_requirements[next_created_index] = new_req;
      // Created regions always return privileges that they make
      returnable_privileges[next_created_index++] = !task_local;
    }

    //--------------------------------------------------------------------------
    void TaskContext::log_created_requirements(void)
    //--------------------------------------------------------------------------
    {
      std::vector<MappingInstance> instances(1, 
            Mapping::PhysicalInstance::get_virtual_instance());
      const UniqueID unique_op_id = get_unique_id();
      for (std::map<unsigned,RegionRequirement>::const_iterator it = 
           created_requirements.begin(); it != created_requirements.end(); it++)
      {
        // We already logged the requirement when we made it
        // Skip it if there are no privilege fields
        if (it->second.privilege_fields.empty())
          continue;
        InstanceSet instance_set;
        std::vector<PhysicalManager*> unacquired;  
        RegionTreeID bad_tree; std::vector<FieldID> missing_fields;
        runtime->forest->physical_convert_mapping(owner_task, 
            it->second, instances, instance_set, bad_tree, 
            missing_fields, NULL, unacquired, false/*do acquire_checks*/);
        runtime->forest->log_mapping_decision(unique_op_id, this,
            it->first, it->second, instance_set);
      }
    } 

    //--------------------------------------------------------------------------
    void TaskContext::register_region_creation(LogicalRegion handle,
                                               const bool task_local,
                                               const bool output_region)
    //--------------------------------------------------------------------------
    {
      // Create a new logical region 
      // Hold the operation lock when doing this since children could
      // be returning values from the utility processor
      AutoLock priv_lock(privilege_lock);
#ifdef DEBUG_LEGION
      assert(local_regions.find(handle) == local_regions.end());
      assert(created_regions.find(handle) == created_regions.end());
#endif
      if (task_local)
      {
        if (is_leaf_context())
          REPORT_LEGION_ERROR(ERROR_ILLEGAL_REGION_CREATION,
              "Illegal task-local region creation performed in leaf task %s "
                           "(ID %lld)", get_task_name(), get_unique_id())
        local_regions[handle] = false/*not deleted*/;
      }
      else
      {
#ifdef DEBUG_LEGION
        assert(created_regions.find(handle) == created_regions.end());
#endif
        created_regions[handle] = 1;
      }
      add_created_region(handle, task_local, output_region);
    }

    //--------------------------------------------------------------------------
    void TaskContext::register_field_creation(FieldSpace handle, 
                                              FieldID fid, bool local)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      std::pair<FieldSpace,FieldID> key(handle,fid);
#ifdef DEBUG_LEGION
      assert(local_fields.find(key) == local_fields.end());
      assert(created_fields.find(key) == created_fields.end());
#endif
      if (!local)
      {
#ifdef DEBUG_LEGION
        assert(created_fields.find(key) == created_fields.end());
#endif
        created_fields.insert(key);
      }
      else
        local_fields[key] = false/*deleted*/;
    }

    //--------------------------------------------------------------------------
    void TaskContext::register_all_field_creations(FieldSpace handle,bool local,
                                             const std::vector<FieldID> &fields)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (local)
      {
        for (unsigned idx = 0; idx < fields.size(); idx++)
        {
          std::pair<FieldSpace,FieldID> key(handle,fields[idx]);
#ifdef DEBUG_LEGION
          assert(local_fields.find(key) == local_fields.end());
#endif
          local_fields[key] = false/*deleted*/;
        }
      }
      else
      {
        for (unsigned idx = 0; idx < fields.size(); idx++)
        {
          std::pair<FieldSpace,FieldID> key(handle,fields[idx]);
#ifdef DEBUG_LEGION
          assert(created_fields.find(key) == created_fields.end());
#endif
          created_fields.insert(key);
        }
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::register_field_space_creation(FieldSpace space)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
#ifdef DEBUG_LEGION
      assert(created_field_spaces.find(space) == created_field_spaces.end());
#endif
      created_field_spaces[space] = 1;
    }

    //--------------------------------------------------------------------------
    bool TaskContext::has_created_index_space(IndexSpace space) const
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      return (created_index_spaces.find(space) != created_index_spaces.end());
    }

    //--------------------------------------------------------------------------
    void TaskContext::register_index_space_creation(IndexSpace space)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
#ifdef DEBUG_LEGION
      // This assertion is not valid anymore because of aliased sharded
      // index spaces in control replication contexts
      //assert(created_index_spaces.find(space) == created_index_spaces.end());
#endif
      created_index_spaces[space] = 1;
    }

    //--------------------------------------------------------------------------
    void TaskContext::register_index_partition_creation(IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
#ifdef DEBUG_LEGION
      assert(created_index_partitions.find(handle) == 
             created_index_partitions.end());
#endif
      created_index_partitions[handle] = 1;
    }

    //--------------------------------------------------------------------------
    void TaskContext::report_leaks_and_duplicates(
                                               std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      if (!deleted_regions.empty())
      {
        for (std::vector<LogicalRegion>::const_iterator it = 
              deleted_regions.begin(); it != deleted_regions.end(); it++)
          REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
              "Duplicate deletions were performed for region (%x,%x,%x) "
              "in task tree rooted by %s", it->index_space.id, 
              it->field_space.id, it->tree_id, get_task_name())
        deleted_regions.clear();
      }
      if (!deleted_fields.empty())
      {
        for (std::vector<std::pair<FieldSpace,FieldID> >::const_iterator it =
              deleted_fields.begin(); it != deleted_fields.end(); it++)
          REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
              "Duplicate deletions were performed on field %d of "
              "field space %x in task tree rooted by %s", it->second, 
              it->first.id, get_task_name())
        deleted_fields.clear();
      }
      if (!deleted_field_spaces.empty())
      {
        for (std::vector<FieldSpace>::const_iterator it = 
              deleted_field_spaces.begin(); it != 
              deleted_field_spaces.end(); it++)
          REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
              "Duplicate deletions were performed on field space %x "
              "in task tree rooted by %s", it->id, get_task_name())
        deleted_field_spaces.clear();
      }
      if (!deleted_index_spaces.empty())
      {
        for (std::vector<std::pair<IndexSpace,bool> >::const_iterator it =
              deleted_index_spaces.begin(); it != 
              deleted_index_spaces.end(); it++)
          REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
              "Duplicate deletions were performed on index space %x "
              "in task tree rooted by %s", it->first.id, get_task_name())
        deleted_index_spaces.clear();
      }
      if (!deleted_index_partitions.empty())
      {
        for (std::vector<std::pair<IndexPartition,bool> >::const_iterator it =
              deleted_index_partitions.begin(); it !=
              deleted_index_partitions.end(); it++)
          REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
              "Duplicate deletions were performed on index partition %x "
              "in task tree rooted by %s", it->first.id, get_task_name())
        deleted_index_partitions.clear();
      }
      // Now we go through and delete anything that the user leaked
      if (!created_regions.empty())
      {
        for (std::map<LogicalRegion,unsigned>::const_iterator it = 
              created_regions.begin(); it != created_regions.end(); it++)
        {
          if (runtime->report_leaks)
            REPORT_LEGION_WARNING(LEGION_WARNING_LEAKED_RESOURCE,
                "Logical region (%x,%x,%x) was leaked out of task tree rooted "
                "by task %s", it->first.index_space.id, 
                it->first.field_space.id, it->first.tree_id, get_task_name())
          runtime->forest->destroy_logical_region(it->first, preconditions);
        }
        created_regions.clear();
      }
      if (!created_fields.empty())
      {
        std::map<FieldSpace,FieldAllocatorImpl*> leak_allocators;
        for (std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
              it = created_fields.begin(); it != created_fields.end(); it++)
        {
          if (runtime->report_leaks)
            REPORT_LEGION_WARNING(LEGION_WARNING_LEAKED_RESOURCE,
                "Field %d of field space %x was leaked out of task tree rooted "
                "by task %s", it->second, it->first.id, get_task_name())
          std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator finder =
              leak_allocators.find(it->first);
          if (finder == leak_allocators.end())
          {
            FieldAllocatorImpl *allocator = 
              create_field_allocator(it->first, true/*unordered*/);
            allocator->add_reference();
            leak_allocators[it->first] = allocator;
            allocator->ready_event.wait();
          }
          else
            finder->second->ready_event.wait();
          runtime->forest->free_field(it->first, it->second, preconditions);
        }
        for (std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator it =
              leak_allocators.begin(); it != leak_allocators.end(); it++)
          if (it->second->remove_reference())
            delete it->second;
        created_fields.clear();
      }
      if (!created_field_spaces.empty())
      {
        for (std::map<FieldSpace,unsigned>::const_iterator it = 
              created_field_spaces.begin(); it != 
              created_field_spaces.end(); it++)
        {
          if (runtime->report_leaks)
            REPORT_LEGION_WARNING(LEGION_WARNING_LEAKED_RESOURCE,
                "Field space %x was leaked out of task tree rooted by task %s",
                it->first.id, get_task_name())
          runtime->forest->destroy_field_space(it->first, preconditions);
        }
        created_field_spaces.clear();
      }
      if (!created_index_partitions.empty())
      {
        for (std::map<IndexPartition,unsigned>::const_iterator it =
              created_index_partitions.begin(); it != 
              created_index_partitions.end(); it++)
        {
          if (runtime->report_leaks)
            REPORT_LEGION_WARNING(LEGION_WARNING_LEAKED_RESOURCE,
                "Index partition %x was leaked out of task tree rooted by "
                "task %s", it->first.id, get_task_name())
          runtime->forest->destroy_index_partition(it->first, preconditions);
        }
        created_index_partitions.clear();
      }
      if (!created_index_spaces.empty())
      {
        for (std::map<IndexSpace,unsigned>::const_iterator it = 
              created_index_spaces.begin(); it !=
              created_index_spaces.end(); it++)
        {
          if (runtime->report_leaks)
            REPORT_LEGION_WARNING(LEGION_WARNING_LEAKED_RESOURCE,
                "Index space %x was leaked out of task tree rooted by task %s",
                it->first.id, get_task_name())
          runtime->forest->destroy_index_space(it->first, preconditions);
        }
        created_index_spaces.clear();
      } 
    }

    //--------------------------------------------------------------------------
    void TaskContext::analyze_destroy_fields(FieldSpace handle,
                                             const std::set<FieldID> &to_delete,
                                    std::vector<RegionRequirement> &delete_reqs,
                                    std::vector<unsigned> &parent_req_indexes,
                                    std::vector<FieldID> &global_to_free,
                                    std::vector<FieldID> &local_to_free,
                                    std::vector<unsigned> &local_field_indexes,
                                    std::vector<unsigned> &deletion_indexes)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!is_leaf_context());
#endif
      {
        // We can't destroy any fields from our original regions because we
        // were not the ones that made them.
        AutoLock priv_lock(privilege_lock);
        // We can actually remove the fields from the data structure now 
        for (std::set<FieldID>::const_iterator it =
              to_delete.begin(); it != to_delete.end(); it++)
        {
          const std::pair<FieldSpace,FieldID> key(handle, *it);
          std::set<std::pair<FieldSpace,FieldID> >::iterator finder = 
            created_fields.find(key);
          if (finder == created_fields.end())
          {
            std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
              local_finder = local_fields.find(key);
#ifdef DEBUG_LEGION
            assert(local_finder != local_fields.end());
            assert(local_finder->second);
#endif
            local_fields.erase(local_finder);
            local_to_free.push_back(*it);
          }
          else
          {
            created_fields.erase(finder);
            global_to_free.push_back(*it);
          }
        }
        // Now figure out which region requirements can be destroyed
        for (std::map<unsigned,RegionRequirement>::iterator it = 
              created_requirements.begin(); it != 
              created_requirements.end(); it++)
        {
          if (it->second.region.get_field_space() != handle)
            continue;
          std::set<FieldID> overlapping_fields;
          for (std::set<FieldID>::const_iterator fit = to_delete.begin();
                fit != to_delete.end(); fit++)
          {
            std::set<FieldID>::const_iterator finder = 
              it->second.privilege_fields.find(*fit);
            if (finder != it->second.privilege_fields.end())
              overlapping_fields.insert(*fit);
          }
          if (overlapping_fields.empty())
            continue;
          delete_reqs.resize(delete_reqs.size()+1);
          RegionRequirement &req = delete_reqs.back();
          req.region = it->second.region;
          req.parent = it->second.region;
          req.privilege = LEGION_READ_WRITE;
          req.prop = LEGION_EXCLUSIVE;
          req.privilege_fields = overlapping_fields;
          req.handle_type = LEGION_SINGULAR_PROJECTION;
          parent_req_indexes.push_back(it->first);
          std::map<unsigned,unsigned>::iterator deletion_finder =
            deletion_counts.find(it->first);
          if (deletion_finder != deletion_counts.end())
          {
            deletion_finder->second++;
            deletion_indexes.push_back(it->first);
          }
          // We need some extra logging for legion spy
          if (runtime->legion_spy_enabled)
          {
            LegionSpy::log_requirement_fields(get_unique_id(),
                                              it->first, overlapping_fields);
            std::vector<MappingInstance> instances(1, 
                          Mapping::PhysicalInstance::get_virtual_instance());
            InstanceSet instance_set;
            std::vector<PhysicalManager*> unacquired;  
            RegionTreeID bad_tree; std::vector<FieldID> missing_fields;
            runtime->forest->physical_convert_mapping(owner_task, 
                req, instances, instance_set, bad_tree, 
                missing_fields, NULL, unacquired, false/*do acquire_checks*/);
            runtime->forest->log_mapping_decision(get_unique_id(), this,
                it->first, req, instance_set);
          }
        }
      }
      if (!local_to_free.empty())
        analyze_free_local_fields(handle, local_to_free, local_field_indexes);
    }

    //--------------------------------------------------------------------------
    void TaskContext::analyze_free_local_fields(FieldSpace handle,
                                     const std::vector<FieldID> &local_to_free,
                                     std::vector<unsigned> &local_field_indexes)
    //--------------------------------------------------------------------------
    {
      // Should only be performed on derived classes
      assert(false);
    }

    //--------------------------------------------------------------------------
    void TaskContext::analyze_destroy_logical_region(LogicalRegion handle,
                                    std::vector<RegionRequirement> &delete_reqs,
                                    std::vector<unsigned> &parent_req_indexes,
                                    std::vector<bool> &returnable)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!is_leaf_context());
#endif
      // If we're deleting a field space then we can't be deleting any of the 
      // original requirements, only requirements that we created
      if (runtime->legion_spy_enabled)
      {
        // We need some extra logging for legion spy
        std::vector<MappingInstance> instances(1, 
              Mapping::PhysicalInstance::get_virtual_instance());
        const UniqueID unique_op_id = get_unique_id();
        AutoLock priv_lock(privilege_lock);
        for (std::map<unsigned,RegionRequirement>::iterator it = 
              created_requirements.begin(); it != 
              created_requirements.end(); /*nothing*/)
        {
          // Has to match precisely
          if (handle.get_tree_id() == it->second.region.get_tree_id())
          {
#ifdef DEBUG_LEGION
            // Should be the same region
            assert(handle == it->second.region);
            assert(returnable_privileges.find(it->first) !=
                    returnable_privileges.end());
#endif
            // Only need to record this if there are privilege fields
            if (!it->second.privilege_fields.empty())
            {
              // Do extra logging for legion spy
              InstanceSet instance_set;
              std::vector<PhysicalManager*> unacquired;  
              RegionTreeID bad_tree; std::vector<FieldID> missing_fields;
              runtime->forest->physical_convert_mapping(owner_task, 
                  it->second, instances, instance_set, bad_tree, 
                  missing_fields, NULL, unacquired, false/*do acquire_checks*/);
              runtime->forest->log_mapping_decision(unique_op_id, this,
                  it->first, it->second, instance_set);
              // Then do the result of the normal operations
              delete_reqs.resize(delete_reqs.size()+1);
              RegionRequirement &req = delete_reqs.back();
              req.region = it->second.region;
              req.parent = it->second.region;
              req.privilege = LEGION_READ_WRITE;
              req.prop = LEGION_EXCLUSIVE;
              req.privilege_fields = it->second.privilege_fields;
              req.handle_type = LEGION_SINGULAR_PROJECTION;
              req.flags = it->second.flags;
              parent_req_indexes.push_back(it->first);
              returnable.push_back(returnable_privileges[it->first]);
              // Always put a deletion index on here to mark that 
              // the requirement is going to be deleted
              std::map<unsigned,unsigned>::iterator deletion_finder =
                deletion_counts.find(it->first);
              if (deletion_finder == deletion_counts.end())
                deletion_counts[it->first] = 1;
              else
                deletion_finder->second++;
              // Can't delete this yet since other deletions might 
              // need to find it until it is finally applied
              it++;
            }
            else // Can erase the returnable privileges now
            {
              returnable_privileges.erase(it->first);
              // Remove the requirement from the created set 
              std::map<unsigned,RegionRequirement>::iterator to_delete = it++;
              created_requirements.erase(to_delete);
            }
          }
          else
            it++;
        }
        // Remove the region from the created set
        {
          std::map<LogicalRegion,unsigned>::iterator finder = 
            created_regions.find(handle);
          if (finder == created_regions.end())
          {
            std::map<LogicalRegion,bool>::iterator local_finder = 
              local_regions.find(handle);
#ifdef DEBUG_LEGION
            assert(local_finder != local_regions.end());
            assert(local_finder->second);
#endif
            local_regions.erase(local_finder);
          }
          else
          {
#ifdef DEBUG_LEGION
            assert(finder->second == 0);
#endif
            created_regions.erase(finder);
          }
        }
        // Check to see if we have any latent field spaces to clean up
        if (!latent_field_spaces.empty())
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder =
            latent_field_spaces.find(handle.get_field_space());
          if (finder != latent_field_spaces.end())
          {
            std::set<LogicalRegion>::iterator region_finder = 
              finder->second.find(handle);
#ifdef DEBUG_LEGION
            assert(region_finder != finder->second.end());
#endif
            finder->second.erase(region_finder);
            if (finder->second.empty())
            {
              // Now that all the regions using this field space have
              // been deleted we can clean up all the created_fields
              for (std::set<std::pair<FieldSpace,FieldID> >::iterator it =
                    created_fields.begin(); it != 
                    created_fields.end(); /*nothing*/)
              {
                if (it->first == finder->first)
                {
                  std::set<std::pair<FieldSpace,FieldID> >::iterator 
                    to_delete = it++;
                  created_fields.erase(to_delete);
                }
                else
                  it++;
              }
              latent_field_spaces.erase(finder);
            }
          }
        }
      }
      else
      {
        AutoLock priv_lock(privilege_lock);
        for (std::map<unsigned,RegionRequirement>::iterator it = 
              created_requirements.begin(); it != 
              created_requirements.end(); /*nothing*/)
        {
          // Has to match precisely
          if (handle.get_tree_id() == it->second.region.get_tree_id())
          {
#ifdef DEBUG_LEGION
            // Should be the same region
            assert(handle == it->second.region);
            assert(returnable_privileges.find(it->first) !=
                    returnable_privileges.end());
#endif
            // Only need to record this if there are privilege fields
            if (!it->second.privilege_fields.empty())
            {
              delete_reqs.resize(delete_reqs.size()+1);
              RegionRequirement &req = delete_reqs.back();
              req.region = it->second.region;
              req.parent = it->second.region;
              req.privilege = LEGION_READ_WRITE;
              req.prop = LEGION_EXCLUSIVE;
              req.privilege_fields = it->second.privilege_fields;
              req.handle_type = LEGION_SINGULAR_PROJECTION;
              parent_req_indexes.push_back(it->first);
              returnable.push_back(returnable_privileges[it->first]);
              // Always put a deletion index on here to mark that 
              // the requirement is going to be deleted
              std::map<unsigned,unsigned>::iterator deletion_finder =
                deletion_counts.find(it->first);
              if (deletion_finder == deletion_counts.end())
                deletion_counts[it->first] = 1;
              else
                deletion_finder->second++;
              // Can't delete this yet until it's actually performed
              // because other deletions might need to depend on it
              it++;
            }
            else // Can erase the returnable privileges now
            {
              returnable_privileges.erase(it->first);
              // Remove the requirement from the created set 
              std::map<unsigned,RegionRequirement>::iterator to_delete = it++;
              created_requirements.erase(to_delete);
            }
          }
          else
            it++;
        }
        // Remove the region from the created set
        {
          std::map<LogicalRegion,unsigned>::iterator finder = 
            created_regions.find(handle);
          if (finder == created_regions.end())
          {
            std::map<LogicalRegion,bool>::iterator local_finder = 
              local_regions.find(handle);
#ifdef DEBUG_LEGION
            assert(local_finder != local_regions.end());
            assert(local_finder->second);
#endif
            local_regions.erase(local_finder);
          }
          else
          {
#ifdef DEBUG_LEGION
            assert(finder->second == 0);
#endif
            created_regions.erase(finder);
          }
        }
        // Check to see if we have any latent field spaces to clean up
        if (!latent_field_spaces.empty())
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder =
            latent_field_spaces.find(handle.get_field_space());
          if (finder != latent_field_spaces.end())
          {
            std::set<LogicalRegion>::iterator region_finder = 
              finder->second.find(handle);
#ifdef DEBUG_LEGION
            assert(region_finder != finder->second.end());
#endif
            finder->second.erase(region_finder);
            if (finder->second.empty())
            {
              // Now that all the regions using this field space have
              // been deleted we can clean up all the created_fields
              for (std::set<std::pair<FieldSpace,FieldID> >::iterator it =
                    created_fields.begin(); it != 
                    created_fields.end(); /*nothing*/)
              {
                if (it->first == finder->first)
                {
                  std::set<std::pair<FieldSpace,FieldID> >::iterator 
                    to_delete = it++;
                  created_fields.erase(to_delete);
                }
                else
                  it++;
              }
              latent_field_spaces.erase(finder);
            }
          }
        }
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::remove_deleted_requirements(
                                          const std::vector<unsigned> &indexes,
                                          std::vector<LogicalRegion> &to_delete)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      for (std::vector<unsigned>::const_iterator it = 
            indexes.begin(); it != indexes.end(); it++) 
      {
        std::map<unsigned,unsigned>::iterator finder = 
          deletion_counts.find(*it);
#ifdef DEBUG_LEGION
        assert(finder != deletion_counts.end());
        assert(finder->second > 0);
#endif
        finder->second--;
        // Check to see if we're the last deletion with this region requirement
        if (finder->second > 0)
          continue;
        deletion_counts.erase(finder); 
        std::map<unsigned,RegionRequirement>::iterator req_finder = 
          created_requirements.find(*it);
#ifdef DEBUG_LEGION
        assert(req_finder != created_requirements.end());
        assert(returnable_privileges.find(*it) != returnable_privileges.end());
#endif
        to_delete.push_back(req_finder->second.parent);
        created_requirements.erase(req_finder);
        returnable_privileges.erase(*it);
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::remove_deleted_fields(const std::set<FieldID> &to_free,
                                           const std::vector<unsigned> &indexes)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      for (std::vector<unsigned>::const_iterator it = 
            indexes.begin(); it != indexes.end(); it++) 
      {
        std::map<unsigned,RegionRequirement>::iterator req_finder = 
          created_requirements.find(*it);
#ifdef DEBUG_LEGION
        assert(req_finder != created_requirements.end());
#endif
        std::set<FieldID> &priv_fields = req_finder->second.privilege_fields;
        if (priv_fields.empty())
          continue;
        for (std::set<FieldID>::const_iterator fit = 
              to_free.begin(); fit != to_free.end(); fit++)
          priv_fields.erase(*fit);
      }
    }

    //--------------------------------------------------------------------------
    void TaskContext::remove_deleted_local_fields(FieldSpace space,
                                          const std::vector<FieldID> &to_remove)
    //--------------------------------------------------------------------------
    {
      // Should only be implemented by derived classes
      assert(false);
    } 

    //--------------------------------------------------------------------------
    void TaskContext::raise_poison_exception(void)
    //--------------------------------------------------------------------------
    {
      // TODO: handle poisoned task
      assert(false);
    }

    //--------------------------------------------------------------------------
    void TaskContext::raise_region_exception(PhysicalRegion region,bool nuclear)
    //--------------------------------------------------------------------------
    {
      // TODO: handle region exception
      assert(false);
    }

    //--------------------------------------------------------------------------
    bool TaskContext::safe_cast(RegionTreeForest *forest, IndexSpace handle,
                                const void *realm_point, TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      // Check to see if we already have the pointer for the node
      std::map<IndexSpace,IndexSpaceNode*>::const_iterator finder =
        safe_cast_spaces.find(handle);
      if (finder == safe_cast_spaces.end())
      {
        safe_cast_spaces[handle] = forest->get_node(handle);
        finder = safe_cast_spaces.find(handle);
      }
      return finder->second->contains_point(realm_point, type_tag);
    }

    //--------------------------------------------------------------------------
    bool TaskContext::is_region_mapped(unsigned idx)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(idx < physical_regions.size());
#endif
      return physical_regions[idx].is_mapped();
    }

    //--------------------------------------------------------------------------
    void TaskContext::clone_requirement(unsigned idx, RegionRequirement &target)
    //--------------------------------------------------------------------------
    {
      if (idx >= regions.size())
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<unsigned,RegionRequirement>::const_iterator finder = 
          created_requirements.find(idx);
#ifdef DEBUG_LEGION
        assert(finder != created_requirements.end());
#endif
        target = finder->second;
      }
      else
        target = regions[idx];
    }

    //--------------------------------------------------------------------------
    int TaskContext::find_parent_region_req(const RegionRequirement &req,
                                            bool check_privilege /*= true*/)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_PARENT_REGION_REQ_CALL);
      // We can check most of our region requirements without the lock
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        const RegionRequirement &our_req = regions[idx];
        // First check that the regions match
        if (our_req.region != req.parent)
          continue;
        // Next check the privileges
        if (check_privilege && 
            ((PRIV_ONLY(req) & our_req.privilege) != PRIV_ONLY(req)))
          continue;
        // Finally check that all the fields are contained
        bool dominated = true;
        for (std::set<FieldID>::const_iterator it = 
              req.privilege_fields.begin(); it !=
              req.privilege_fields.end(); it++)
        {
          if (our_req.privilege_fields.find(*it) ==
              our_req.privilege_fields.end())
          {
            dominated = false;
            break;
          }
        }
        if (!dominated)
          continue;
        return int(idx);
      }
      const FieldSpace fs = req.parent.get_field_space(); 
      // The created region requirements have to be checked while holding
      // the lock since they are subject to mutation by the application
      // We might also mutate it so we take the lock in exclusive mode
      AutoLock priv_lock(privilege_lock);
      for (std::map<unsigned,RegionRequirement>::iterator it = 
           created_requirements.begin(); it != created_requirements.end(); it++)
      {
        RegionRequirement &our_req = it->second;
        // First check that the regions match
        if (our_req.region != req.parent)
          continue;
        // Next check the privileges
        if (check_privilege && 
            ((PRIV_ONLY(req) & our_req.privilege) != PRIV_ONLY(req)))
          continue;
#ifdef DEBUG_LEGION
        assert(returnable_privileges.find(it->first) != 
                returnable_privileges.end());
#endif
        // If this is a returnable privilege requiremnt that means
        // that we made this region so we always have privileges
        // on any fields for that region, just add them and be done
        if (returnable_privileges[it->first])
        {
          our_req.privilege_fields.insert(req.privilege_fields.begin(),
                                          req.privilege_fields.end());
          return it->first;
        }
        // Finally check that all the fields are contained
        bool dominated = true;
        for (std::set<FieldID>::const_iterator fit = 
              req.privilege_fields.begin(); fit !=
              req.privilege_fields.end(); fit++)
        {
          if (our_req.privilege_fields.find(*fit) ==
              our_req.privilege_fields.end())
          {
            // Check to see if this is a field we made
            // and haven't destroyed yet
            std::pair<FieldSpace,FieldID> key(fs, *fit);
            if (created_fields.find(key) != created_fields.end())
            {
              // We made it so we can add it to the requirement
              // and continue on our way
              our_req.privilege_fields.insert(*fit);
              continue;
            }
            if (local_fields.find(key) != local_fields.end())
            {
              // We made it so we can add it to the requirement
              // and continue on our way
              our_req.privilege_fields.insert(*fit);
              continue;
            }
            // Otherwise we don't have privileges
            dominated = false;
            break;
          }
        }
        if (!dominated)
          continue;
        // Include the offset by the number of base requirements
        return it->first;
      }
      // Method of last resort, check to see if we made all the fields
      // if we did, then we can make a new requirement for all the fields
      for (std::set<FieldID>::const_iterator it = req.privilege_fields.begin();
            it != req.privilege_fields.end(); it++)
      {
        std::pair<FieldSpace,FieldID> key(fs, *it);
        // Didn't make it so we don't have privileges anywhere
        if ((created_fields.find(key) == created_fields.end()) &&
            (local_fields.find(key) == local_fields.end()))
          return -1;
      }
      // If we get here then we can make a new requirement
      // which has non-returnable privileges
      // Get the top level region for the region tree
      RegionNode *top = runtime->forest->get_tree(req.parent.get_tree_id());
      const unsigned index = next_created_index++;
      RegionRequirement &new_req = created_requirements[index];
      new_req = RegionRequirement(top->handle, LEGION_READ_WRITE, 
                                  LEGION_EXCLUSIVE, top->handle);
      if (runtime->legion_spy_enabled)
        TaskOp::log_requirement(get_unique_id(), index, new_req);
      // Add our fields
      new_req.privilege_fields.insert(
          req.privilege_fields.begin(), req.privilege_fields.end());
      // This is not a returnable privilege requirement
      returnable_privileges[index] = false;
      return index;
    }

    //--------------------------------------------------------------------------
    LegionErrorType TaskContext::check_privilege(
                                         const IndexSpaceRequirement &req) const
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, CHECK_PRIVILEGE_CALL);
      if (req.verified)
        return NO_ERROR;
      // Find the parent index space
      for (std::vector<IndexSpaceRequirement>::const_iterator it = 
            owner_task->indexes.begin(); it != owner_task->indexes.end(); it++)
      {
        // Check to see if we found the requirement in the parent 
        if (it->handle == req.parent)
        {
          // Check that there is a path between the parent and the child
          std::vector<LegionColor> path;
          if (!runtime->forest->compute_index_path(req.parent, 
                                                   req.handle, path))
            return ERROR_BAD_INDEX_PATH;
          // Now check that the privileges are less than or equal
          if (req.privilege & (~(it->privilege)))
          {
            return ERROR_BAD_INDEX_PRIVILEGES;  
          }
          return NO_ERROR;
        }
      }
      // If we didn't find it here, we have to check the added 
      // index spaces that we have
      if (has_created_index_space(req.parent))
      {
        // Still need to check that there is a path between the two
        std::vector<LegionColor> path;
        if (!runtime->forest->compute_index_path(req.parent, req.handle, path))
          return ERROR_BAD_INDEX_PATH;
        // No need to check privileges here since it is a created space
        // which means that the parent has all privileges.
        return NO_ERROR;
      }
      return ERROR_BAD_PARENT_INDEX;
    }

    //--------------------------------------------------------------------------
    LegionErrorType TaskContext::check_privilege(const RegionRequirement &req,
                                                 FieldID &bad_field,
                                                 int &bad_index,
                                                 bool skip_privilege) const
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, CHECK_PRIVILEGE_CALL);
#ifdef DEBUG_LEGION
      assert(bad_index < 0);
#endif
      if (req.flags & LEGION_VERIFIED_FLAG)
        return NO_ERROR;
      // Copy privilege fields for check
      std::set<FieldID> privilege_fields(req.privilege_fields);
      // Try our original region requirements first
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        LegionErrorType et = 
          check_privilege_internal(req, regions[idx], privilege_fields, 
                                   bad_field, idx, bad_index, skip_privilege);
        // No error so we are done
        if (et == NO_ERROR)
          return et;
        // Something other than bad parent region is a real error
        if (et != ERROR_BAD_PARENT_REGION)
          return et;
        // Otherwise we just keep going
      }
      // If none of that worked, we now get to try the created requirements
      AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
      for (std::map<unsigned,RegionRequirement>::const_iterator it = 
            created_requirements.begin(); it != 
            created_requirements.end(); it++)
      {
        const RegionRequirement &created_req = it->second;
        LegionErrorType et = 
          check_privilege_internal(req, created_req, privilege_fields, 
                      bad_field, it->first, bad_index, skip_privilege);
        // No error so we are done
        if (et == NO_ERROR)
          return et;
        // Something other than bad parent region is a real error
        if (et != ERROR_BAD_PARENT_REGION)
          return et;
        // If we got a BAD_PARENT_REGION, see if this a returnable
        // privilege in which case we know we have privileges on all fields
        if (created_req.privilege_fields.empty())
        {
          // Still have to check the parent region is right
          if (req.parent == created_req.region)
            return NO_ERROR;
        }
        // Otherwise we just keep going
      }
      // Finally see if we created all the fields in which case we know
      // we have privileges on all their regions
      const FieldSpace sp = req.parent.get_field_space();
      for (std::set<FieldID>::const_iterator it = req.privilege_fields.begin();
            it != req.privilege_fields.end(); it++)
      {
        std::pair<FieldSpace,FieldID> key(sp, *it);
        // If we don't find the field, then we are done
        if ((created_fields.find(key) == created_fields.end()) &&
            (local_fields.find(key) == local_fields.end()))
          return ERROR_BAD_PARENT_REGION;
      }
      // Check that the parent is the root of the tree, if not it is bad
      RegionNode *parent_region = runtime->forest->get_node(req.parent);
      if (parent_region->parent != NULL)
        return ERROR_BAD_PARENT_REGION;
      // Otherwise we have privileges on these fields for all regions
      // so we are good on privileges
      return NO_ERROR;
    } 

    //--------------------------------------------------------------------------
    LegionErrorType TaskContext::check_privilege_internal(
        const RegionRequirement &req, const RegionRequirement &our_req,
        std::set<FieldID>& privilege_fields, FieldID &bad_field, 
        int local_index, int &bad_index, bool skip_privilege) const
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
      // Check to see if we found the requirement in the parent
      if (our_req.region == req.parent)
      {
        // If we make it in here then we know we have at least found
        // the parent name so we can set the bad index
        bad_index = local_index;
        bad_field = LEGION_AUTO_GENERATE_ID; // set it to an invalid field
        if ((req.handle_type == LEGION_SINGULAR_PROJECTION) || 
            (req.handle_type == LEGION_REGION_PROJECTION))
        {
          std::vector<LegionColor> path;
          if (!runtime->forest->compute_index_path(req.parent.index_space,
                                            req.region.index_space, path))
            return ERROR_BAD_REGION_PATH;
        }
        else
        {
          std::vector<LegionColor> path;
          if (!runtime->forest->compute_partition_path(req.parent.index_space,
                                        req.partition.index_partition, path))
            return ERROR_BAD_PARTITION_PATH;
        }
        // Now check that the types are subset of the fields
        // Note we can use the parent since all the regions/partitions
        // in the same region tree have the same field space
        for (std::set<FieldID>::iterator fit = privilege_fields.begin();
              fit != privilege_fields.end(); /*nothing*/)
        {
          if (our_req.privilege_fields.find(*fit) != 
              our_req.privilege_fields.end())
          {
            // Only need to do this check if there were overlapping fields
            if (!skip_privilege && (PRIV_ONLY(req) & (~(our_req.privilege))))
            {
              if ((req.handle_type == LEGION_SINGULAR_PROJECTION) || 
                  (req.handle_type == LEGION_REGION_PROJECTION))
                return ERROR_BAD_REGION_PRIVILEGES;
              else
                return ERROR_BAD_PARTITION_PRIVILEGES;
            }
            std::set<FieldID>::iterator to_delete = fit++;
            privilege_fields.erase(to_delete);
          }
          else
            fit++;
        }
      }

      if (!privilege_fields.empty()) 
      {
        bad_field = *(privilege_fields.begin());
        return ERROR_BAD_PARENT_REGION;
      }
        // If we make it here then we are good
      return NO_ERROR;
    }

    //--------------------------------------------------------------------------
    bool TaskContext::check_region_dependence(RegionTreeID our_tid,
                                              IndexSpace our_space,
                                              const RegionRequirement &our_req,
                                              const RegionUsage &our_usage,
                                              const RegionRequirement &req,
                                              bool check_privileges) const
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, CHECK_REGION_DEPENDENCE_CALL);
      if ((req.handle_type == LEGION_SINGULAR_PROJECTION) || 
          (req.handle_type == LEGION_REGION_PROJECTION))
      {
        // If the trees are different we're done 
        if (our_tid != req.region.get_tree_id())
          return false;
        // Check to see if there is a path between
        // the index spaces
        std::vector<LegionColor> path;
        if (!runtime->forest->compute_index_path(our_space,
                         req.region.get_index_space(),path))
          return false;
      }
      else
      {
        // Check if the trees are different
        if (our_tid != req.partition.get_tree_id())
          return false;
        std::vector<LegionColor> path;
        if (!runtime->forest->compute_partition_path(our_space,
                     req.partition.get_index_partition(), path))
          return false;
      }
      // Check to see if any privilege fields overlap
      std::vector<FieldID> intersection(our_req.privilege_fields.size());
      std::vector<FieldID>::iterator intersect_it = 
        std::set_intersection(our_req.privilege_fields.begin(),
                              our_req.privilege_fields.end(),
                              req.privilege_fields.begin(),
                              req.privilege_fields.end(),
                              intersection.begin());
      intersection.resize(intersect_it - intersection.begin());
      if (intersection.empty())
        return false;
      // If we aren't supposed to check privileges then we're done
      if (!check_privileges)
        return true;
      // Finally if everything has overlapped, do a dependence analysis
      // on the privileges and coherence
      RegionUsage usage(req);
      switch (check_dependence_type<true>(our_usage,usage))
      {
        // Only allow no-dependence, or simultaneous dependence through
        case LEGION_NO_DEPENDENCE:
        case LEGION_SIMULTANEOUS_DEPENDENCE:
          {
            return false;
          }
        default:
          break;
      }
      return true;
    }

    //--------------------------------------------------------------------------
    LogicalRegion TaskContext::find_logical_region(unsigned index)
    //--------------------------------------------------------------------------
    {
      if (index < regions.size())
        return regions[index].region;
      AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
      std::map<unsigned,RegionRequirement>::const_iterator finder = 
        created_requirements.find(index);
#ifdef DEBUG_LEGION
      assert(finder != created_requirements.end());
#endif
      return finder->second.region;
    } 

    //--------------------------------------------------------------------------
    const std::vector<PhysicalRegion>& TaskContext::begin_task(
                                                           Legion::Runtime *&rt)
    //--------------------------------------------------------------------------
    {
      implicit_context = this;
      implicit_runtime = this->runtime;
      rt = this->runtime->external;
      implicit_provenance = owner_task->get_unique_op_id();
      if (overhead_tracker != NULL)
        previous_profiling_time = Realm::Clock::current_time_in_nanoseconds();
      // Switch over the executing processor to the one
      // that has actually been assigned to run this task.
      executing_processor = Processor::get_executing_processor();
      owner_task->current_proc = executing_processor;
      if (runtime->legion_spy_enabled)
        LegionSpy::log_task_processor(get_unique_id(), executing_processor.id);
#ifdef DEBUG_LEGION
      log_task.debug("Task %s (ID %lld) starting on processor " IDFMT "",
                    get_task_name(), get_unique_id(), executing_processor.id);
      assert(regions.size() == physical_regions.size());
#endif
      // Issue a utility task to decrement the number of outstanding
      // tasks now that this task has started running
      if (!inline_task)
        pending_done = find_parent_context()->decrement_pending(owner_task);
      return physical_regions;
    }

    //--------------------------------------------------------------------------
    PhysicalInstance TaskContext::create_task_local_instance(Memory memory, 
                                           Realm::InstanceLayoutGeneric *layout)
    //--------------------------------------------------------------------------
    {
      PhysicalInstance instance;
      Realm::ProfilingRequestSet no_requests;
#ifdef LEGION_MALLOC_INSTANCES
      uintptr_t ptr = runtime->allocate_deferred_instance(memory, 
                              layout->bytes_used, false/*free*/); 
      const RtEvent wait_on(Realm::RegionInstance::create_external(instance,
                                          memory, ptr, layout, no_requests));
      task_local_instances.push_back(std::make_pair(instance, ptr));
#else
      MemoryManager *manager = runtime->find_memory_manager(memory);
      const RtEvent wait_on(manager->create_eager_instance(instance, layout));
      if (!instance.exists())
      {
        const char *mem_names[] = {
#define MEM_NAMES(name, desc) desc,
          REALM_MEMORY_KINDS(MEM_NAMES) 
#undef MEM_NAMES
        };
        REPORT_LEGION_ERROR(ERROR_DEFERRED_ALLOCATION_FAILURE,
            "Failed to allocate DeferredBuffer/Value/Reduction in task %s "
            "(UID %lld) because %s memory " IDFMT " is full. This is an eager "
            "allocation so you must adjust the percentage of this memory "
            "dedicated for eager allocations with '-lg:eager_alloc_percentage' "
            "flag on the command line.", get_task_name(), get_unique_id(), 
            mem_names[memory.kind()], memory.id)
      }
      task_local_instances.insert(instance);
#endif
      if (wait_on.exists() && !wait_on.has_triggered())
        wait_on.wait();
      return instance;
    }

    //--------------------------------------------------------------------------
    void TaskContext::destroy_task_local_instance(PhysicalInstance instance)
    //--------------------------------------------------------------------------
    {
#ifdef LEGION_MALLOC_INSTANCES
      // TODO: We don't eagerly destroy local instances when they are malloc'ed
#else
      std::set<PhysicalInstance>::iterator finder =
        task_local_instances.find(instance);
#ifdef DEBUG_LEGION
      assert(finder != task_local_instances.end());
#endif
      task_local_instances.erase(finder);
      MemoryManager *manager = runtime->find_memory_manager(
          instance.get_location());
      manager->free_eager_instance(instance, RtEvent::NO_RT_EVENT);
#endif
    }

    //--------------------------------------------------------------------------
    void TaskContext::end_task(const void *res, size_t res_size, bool owned,
     PhysicalInstance deferred_result_instance, FutureFunctor *callback_functor,
                       Memory::Kind result_kind, void (*freefunc)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      // Finalize output regions by setting realm instances created during
      // task execution to the output regions' physical managers
      if (!output_regions.empty())
        finalize_output_regions();
      // If we have a deferred result instance we need to escape that too
      RtEvent copy_future;
      FutureInstance *instance = NULL;
      if (deferred_result_instance.exists())
      {
#ifdef DEBUG_LEGION
        assert(res != NULL);
        assert(callback_functor == NULL);
        assert(freefunc == NULL);
#endif
        instance = new FutureInstance(res, res_size,
            deferred_result_instance.get_location(),
            ApEvent(Processor::get_current_finish_event()), runtime,
            true/*eager*/, false/*external*/, true/*own alloc*/,
            deferred_result_instance);
      }
      else if (res_size > 0)
      {
#ifdef DEBUG_LEGION
        assert(res != NULL);
        assert(callback_functor == NULL);
#endif
        // We've actually got data to pass back, get the memory
        const Memory memory =
          runtime->find_local_memory(executing_processor, result_kind);
        if (owned)
          instance = new FutureInstance(res, res_size, memory,
              ApEvent::NO_AP_EVENT, runtime, false/*eager*/,
              true/*external allocation*/, true/*own allocation*/,
              PhysicalInstance::NO_INST, freefunc, executing_processor);
        else
          instance = copy_to_future_inst(res, res_size, memory, copy_future);
      }
      else if ((callback_functor != NULL) && owner_task->is_reducing_future())
      {
        // If we're reducing this future value then just do the callback
        // now since there is no point in deferring it
        const size_t callback_size = 
          callback_functor->callback_get_future_size();
        void *buffer = malloc(callback_size);
        callback_functor->callback_pack_future(buffer, callback_size);
        callback_functor->callback_release_future();
        if (owned)
          delete callback_functor;
        instance = FutureInstance::create_local(buffer, callback_size,
                                      true/*own allocation*/, runtime);
      }
      // Once there are no more escaping instances we can release the rest
      if (!task_local_instances.empty())
        release_task_local_instances();
      // Mark that we are done executing this operation
      // We're not actually done until we have registered our pending
      // decrement of our parent task and recorded any profiling
      if (!pending_done.has_triggered())
        owner_task->complete_execution(pending_done);
      else
        owner_task->complete_execution();
      // Grab some information before doing the next step in case it
      // results in the deletion of 'this'
#ifdef DEBUG_LEGION
      assert(owner_task != NULL);
      const TaskID owner_task_id = owner_task->task_id;
#endif
      Runtime *runtime_ptr = runtime;
      // Tell the parent context that we are ready for post-end
      InnerContext *parent_ctx = owner_task->get_context();
      const bool internal_task = Processor::get_executing_processor().exists();
      RtEvent effects_done(internal_task && !inline_task ?
          Processor::get_current_finish_event() : Realm::Event::NO_EVENT);
      if (last_registration.exists() && !last_registration.has_triggered())
      {
        if (copy_future.exists() && !copy_future.has_triggered())
          effects_done = 
            Runtime::merge_events(effects_done, last_registration, copy_future);
        else
          effects_done = Runtime::merge_events(effects_done, last_registration);
      }
      else if (copy_future.exists() && !copy_future.has_triggered())
        effects_done = Runtime::merge_events(effects_done, copy_future);
      parent_ctx->add_to_post_task_queue(this, effects_done, instance,
                                         callback_functor, owned);
      if (!inline_task)
#ifdef DEBUG_LEGION
        runtime_ptr->decrement_total_outstanding_tasks(owner_task_id, 
                                                       false/*meta*/);
#else
        runtime_ptr->decrement_total_outstanding_tasks();
#endif
      // If we have a copy future, but don't own the source data we must wait
      // before returning to ensure the copy is done before source is deleted
      if (!owned && copy_future.exists() && !copy_future.has_triggered())
        copy_future.wait();
    }

    //--------------------------------------------------------------------------
    FutureInstance* TaskContext::copy_to_future_inst(const void *value,
                                      size_t size, Memory memory, RtEvent &done)
    //--------------------------------------------------------------------------
    {
      // See if we need to make an eager instance for this or not
      if ((size > LEGION_MAX_RETURN_SIZE) ||
          !FutureInstance::check_meta_visible(runtime, memory))
      {
        // create an eager instance in the chosen memory
        MemoryManager *manager = runtime->find_memory_manager(memory);
        const ApUserEvent ready = Runtime::create_ap_user_event(NULL);
        FutureInstance *instance = 
          manager->create_future_instance(owner_task,ready,size,true/*eager*/);
        // create an external instance for the current allocation
        const std::vector<Realm::FieldID> fids(1, 0/*field id*/);
        const std::vector<size_t> sizes(1, size);
        const int dim_order[1] = { 0 };
        const Realm::InstanceLayoutConstraints constraints(fids, sizes, 1);
        const Realm::IndexSpace<1,coord_t> rect_space(
            Realm::Rect<1,coord_t>(Realm::Point<1,coord_t>(0),
                                   Realm::Point<1,coord_t>(0)));
        Realm::InstanceLayoutGeneric *ilg =
          Realm::InstanceLayoutGeneric::choose_instance_layout<1,coord_t>(
              rect_space, constraints, dim_order);
        PhysicalInstance source_instance;
        const ApEvent src_ready(
            PhysicalInstance::create_external_instance(
              source_instance, memory, ilg, 
              Realm::ExternalMemoryResource(
               reinterpret_cast<uintptr_t>(value), size, true/*read only*/),
              Realm::ProfilingRequestSet()));
        FutureInstance source(value, size, memory, ApEvent::NO_AP_EVENT,runtime,
           false/*eager*/,false/*external*/,false/*own alloc*/,source_instance);
        // issue the copy between them
        Runtime::trigger_event(NULL, ready, 
            instance->copy_from(&source, owner_task));
        done = Runtime::protect_event(ready);
        // deferred delete the external instance source
        source_instance.destroy(done);
        return instance;
      }
      else
      {
        // Make a simple memory copy here now
        void *buffer = malloc(size);
        memcpy(buffer, value, size);
        if (memory.kind() != Memory::SYSTEM_MEM)
          memory = runtime->runtime_system_memory;
        return new FutureInstance(buffer, size, memory, ApEvent::NO_AP_EVENT,
            runtime, false/*eager*/, true/*external*/, true/*own allocation*/,
            PhysicalInstance::NO_INST, NULL, executing_processor);
      }
    }

    //--------------------------------------------------------------------------
    FutureInstance* TaskContext::copy_to_future_inst(Memory memory,
                                                     FutureInstance *source)
    //--------------------------------------------------------------------------
    {
      // See if we need to make an eager instance for this or not
      if ((source->size > LEGION_MAX_RETURN_SIZE) || !source->is_meta_visible ||
          !FutureInstance::check_meta_visible(runtime, memory))
      {
        // create an eager instance in the chosen memory
        MemoryManager *manager = runtime->find_memory_manager(memory);
        const ApUserEvent ready = Runtime::create_ap_user_event(NULL);
        FutureInstance *instance = 
          manager->create_future_instance(owner_task, ready,
                                          source->size, true/*eager*/);
        // issue the copy between them
        Runtime::trigger_event(NULL, ready, 
            instance->copy_from(source, owner_task));
        return instance;
      }
      else
      {
        // Make a simple memory copy here now
        void *buffer = malloc(source->size);
        memcpy(buffer, source->data, source->size);
        if (memory.kind() != Memory::SYSTEM_MEM)
          memory = runtime->runtime_system_memory;
        return new FutureInstance(buffer, source->size, memory, 
                                  ApEvent::NO_AP_EVENT, runtime, false/*eager*/,
                                  true/*external*/, true/*own allocation*/,
                                  PhysicalInstance::NO_INST, NULL,
                                  executing_processor);
      }
    }

    //--------------------------------------------------------------------------
    uintptr_t TaskContext::escape_task_local_instance(PhysicalInstance instance)
    //--------------------------------------------------------------------------
    {
#ifdef LEGION_MALLOC_INSTANCES
      uintptr_t ptr = 0;
      std::vector<std::pair<PhysicalInstance,uintptr_t> > new_instances;
#ifdef DEBUG_LEGION
      assert(!task_local_instances.empty());
#endif
      new_instances.reserve(task_local_instances.size() - 1);
      for (std::vector<std::pair<PhysicalInstance,uintptr_t> >::iterator it =
           task_local_instances.begin(); it != task_local_instances.end(); ++it)
        if (it->first == instance)
          ptr = it->second;
        else
          new_instances.push_back(*it);

#ifdef DEBUG_LEGION
      assert(ptr != 0);
#endif
      task_local_instances.swap(new_instances);
      return ptr;
#else
      std::set<PhysicalInstance>::iterator finder =
        task_local_instances.find(instance);
#ifdef DEBUG_LEGION
      assert(finder != task_local_instances.end());
#endif
      // Remove the instance from the set of task local instances
      task_local_instances.erase(finder);
      void *ptr = instance.pointer_untyped(0,0);
#ifdef DEBUG_LEGION
      assert(ptr != NULL);
#endif
      return reinterpret_cast<uintptr_t>(ptr);
#endif
    }

    //--------------------------------------------------------------------------
    void TaskContext::begin_misspeculation(void)
    //--------------------------------------------------------------------------
    {
      // Issue a utility task to decrement the number of outstanding
      // tasks now that this task has started running
      pending_done = owner_task->get_context()->decrement_pending(owner_task);
    }

    //--------------------------------------------------------------------------
    void TaskContext::end_misspeculation(FutureInstance *instance)
    //--------------------------------------------------------------------------
    {
      // Mark that we are done executing this operation
      owner_task->complete_execution();
      // Grab some information before doing the next step in case it
      // results in the deletion of 'this'
#ifdef DEBUG_LEGION
      assert(owner_task != NULL);
      const TaskID owner_task_id = owner_task->task_id;
#endif
      Runtime *runtime_ptr = runtime;
      // Call post end task
      post_end_task(instance, NULL/*functor*/, false/*owner*/);
#ifdef DEBUG_LEGION
      runtime_ptr->decrement_total_outstanding_tasks(owner_task_id, 
                                                     false/*meta*/);
#else
      runtime_ptr->decrement_total_outstanding_tasks();
#endif
    }

    //--------------------------------------------------------------------------
    void TaskContext::initialize_overhead_tracker(void)
    //--------------------------------------------------------------------------
    {
      // Make an overhead tracker
#ifdef DEBUG_LEGION
      assert(overhead_tracker == NULL);
#endif
      overhead_tracker = new 
        Mapping::ProfilingMeasurements::RuntimeOverhead();
    } 

    //--------------------------------------------------------------------------
    void TaskContext::remap_unmapped_regions(LegionTrace *trace,
                            const std::vector<PhysicalRegion> &unmapped_regions)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!unmapped_regions.empty());
#endif
      if ((trace != NULL) && trace->is_static_trace())
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RUNTIME_REMAPPING,
          "Illegal runtime remapping in static trace inside of "
                      "task %s (UID %lld). Static traces must perfectly "
                      "manage their physical mappings with no runtime help.",
                      get_task_name(), get_unique_id())
      std::set<ApEvent> mapped_events;
      for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
      {
        const ApEvent ready = remap_region(unmapped_regions[idx]);
        if (ready.exists())
          mapped_events.insert(ready);
      }
      // Wait for all the re-mapping operations to complete
      const ApEvent mapped_event = Runtime::merge_events(NULL, mapped_events);
      bool poisoned = false;
      if (mapped_event.has_triggered_faultaware(poisoned))
      {
        if (poisoned)
          raise_poison_exception();
        return;
      }
      begin_task_wait(true/*from runtime*/);
      mapped_event.wait_faultaware(poisoned);
      if (poisoned)
        raise_poison_exception();
      end_task_wait();
    }

    //--------------------------------------------------------------------------
    void* TaskContext::get_local_task_variable(LocalVariableID id)
    //--------------------------------------------------------------------------
    {
      std::map<LocalVariableID,std::pair<void*,void (*)(void*)> >::
        const_iterator finder = task_local_variables.find(id);
      if (finder == task_local_variables.end())
        REPORT_LEGION_ERROR(ERROR_UNABLE_FIND_TASK_LOCAL,
          "Unable to find task local variable %d in task %s "
                      "(UID %lld)", id, get_task_name(), get_unique_id())  
      return finder->second.first;
    }

    //--------------------------------------------------------------------------
    void TaskContext::set_local_task_variable(LocalVariableID id,
                                              const void *value,
                                              void (*destructor)(void*))
    //--------------------------------------------------------------------------
    {
      std::map<LocalVariableID,std::pair<void*,void (*)(void*)> >::iterator
        finder = task_local_variables.find(id);
      if (finder != task_local_variables.end())
      {
        // See if we need to clean things up first
        if (finder->second.second != NULL)
          (*finder->second.second)(finder->second.first);
        finder->second = 
          std::pair<void*,void (*)(void*)>(const_cast<void*>(value),destructor);
      }
      else
        task_local_variables[id] = 
          std::pair<void*,void (*)(void*)>(const_cast<void*>(value),destructor);
    }

    //--------------------------------------------------------------------------
    void TaskContext::yield(void)
    //--------------------------------------------------------------------------
    {
      YieldArgs args(owner_task->get_unique_id());
      // Run this task with minimum priority to allow other things to run
      const RtEvent wait_for = 
        runtime->issue_runtime_meta_task(args, LG_MIN_PRIORITY);
      begin_task_wait(false/*from runtime*/);
      wait_for.wait();
      end_task_wait();
    }

    //--------------------------------------------------------------------------
    void TaskContext::release_task_local_instances(void)
    //--------------------------------------------------------------------------
    {
#ifdef LEGION_MALLOC_INSTANCES
      for (unsigned idx = 0; idx < task_local_instances.size(); idx++)
      {
        std::pair<PhysicalInstance,uintptr_t> inst = task_local_instances[idx];
        inst.first.destroy(Processor::get_current_finish_event());
      }
#else
      for (std::set<PhysicalInstance>::iterator it =
           task_local_instances.begin(); it !=
           task_local_instances.end(); ++it)
      {
        PhysicalInstance inst = *it;
        MemoryManager *manager =
          runtime->find_memory_manager(inst.get_location());
        manager->free_eager_instance(
            inst, RtEvent(Processor::get_current_finish_event()));
      }
#endif
      task_local_instances.clear();
    }

    //--------------------------------------------------------------------------
    Future TaskContext::predicate_task_false(const TaskLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      if (launcher.predicate_false_future.impl != NULL)
        return launcher.predicate_false_future;
      // Otherwise check to see if we have a value
      FutureImpl *result = new FutureImpl(runtime, true/*register*/,
        runtime->get_available_distributed_id(), 
        runtime->address_space, ApEvent::NO_AP_EVENT);
      if (launcher.predicate_false_result.get_size() > 0)
        result->set_local(launcher.predicate_false_result.get_ptr(),
            launcher.predicate_false_result.get_size(), false/*own*/);
      else
      {
        // We need to check to make sure that the task actually
        // does expect to have a void return type
        TaskImpl *impl = runtime->find_or_create_task_impl(launcher.task_id);
        if (impl->returns_value())
          REPORT_LEGION_ERROR(ERROR_PREDICATED_TASK_LAUNCH_FOR_TASK,
            "Predicated task launch for task %s in parent "
                        "task %s (UID %lld) has non-void return type "
                        "but no default value for its future if the task "
                        "predicate evaluates to false.  Please set either "
                        "the 'predicate_false_result' or "
                        "'predicate_false_future' fields of the "
                        "TaskLauncher struct.", impl->get_name(), 
                        get_task_name(), get_unique_id())
        result->set_result(NULL);
      }
      return Future(result);
    }

    //--------------------------------------------------------------------------
    FutureMap TaskContext::predicate_index_task_false(size_t context_index,
                                              const IndexTaskLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      Domain launch_domain = launcher.launch_domain;
      if (!launch_domain.exists())
        runtime->forest->find_launch_space_domain(launcher.launch_space,
                                                  launch_domain);
      FutureMapImpl *result = new FutureMapImpl(this, runtime,
          launch_domain, runtime->get_available_distributed_id(),
          context_index, runtime->address_space, RtEvent::NO_RT_EVENT);
      if (launcher.predicate_false_future.impl != NULL)
      {
        FutureInstance *canonical = 
          launcher.predicate_false_future.impl->get_canonical_instance();
        if (canonical != NULL)
        {
          const Memory target = runtime->find_local_memory(executing_processor,
                                                      canonical->memory.kind());
          for (Domain::DomainPointIterator itr(launcher.launch_domain);
                itr; itr++)
          {
            Future f = result->get_future(itr.p, true/*internal*/);
            f.impl->set_result(copy_to_future_inst(target, canonical));
          }
        }
        else
        {
          for (Domain::DomainPointIterator itr(launcher.launch_domain);
                itr; itr++)
          {
            Future f = result->get_future(itr.p, true/*internal*/);
            f.impl->set_result(NULL);
          }
        }
        return FutureMap(result);
      }
      if (launcher.predicate_false_result.get_size() == 0)
      {
        // Check to make sure the task actually does expect to
        // have a void return type
        TaskImpl *impl = runtime->find_or_create_task_impl(launcher.task_id);
        if (impl->returns_value())
          REPORT_LEGION_ERROR(ERROR_PREDICATED_INDEX_TASK_LAUNCH,
            "Predicated index task launch for task %s "
                        "in parent task %s (UID %lld) has non-void "
                        "return type but no default value for its "
                        "future if the task predicate evaluates to "
                        "false.  Please set either the "
                        "'predicate_false_result' or "
                        "'predicate_false_future' fields of the "
                        "IndexTaskLauncher struct.", impl->get_name(),
                        get_task_name(), get_unique_id())
        // Just initialize all the futures
        for (Domain::DomainPointIterator itr(launcher.launch_domain);
              itr; itr++)
        {
          Future f = result->get_future(itr.p, true/*internal*/);
          f.impl->set_result(NULL);
        }
      }
      else
      {
        const void *ptr = launcher.predicate_false_result.get_ptr();
        size_t ptr_size = launcher.predicate_false_result.get_size();
        for (Domain::DomainPointIterator itr(launcher.launch_domain);
              itr; itr++)
        {
          Future f = result->get_future(itr.p, true/*internal*/);
          f.impl->set_local(ptr, ptr_size, false/*own*/);
        }
      }
      return FutureMap(result);
    }

    //--------------------------------------------------------------------------
    Future TaskContext::predicate_index_task_reduce_false(
                                              const IndexTaskLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      if (launcher.predicate_false_future.impl != NULL)
        return launcher.predicate_false_future;
      // Otherwise check to see if we have a value
      FutureImpl *result = new FutureImpl(runtime, true/*register*/, 
        runtime->get_available_distributed_id(), 
        runtime->address_space, ApEvent::NO_AP_EVENT);
      if (launcher.predicate_false_result.get_size() > 0)
        result->set_local(launcher.predicate_false_result.get_ptr(),
            launcher.predicate_false_result.get_size(), false/*own*/);
      else
      {
        // We need to check to make sure that the task actually
        // does expect to have a void return type
        TaskImpl *impl = runtime->find_or_create_task_impl(launcher.task_id);
        if (impl->returns_value())
          REPORT_LEGION_ERROR(ERROR_PREDICATED_INDEX_TASK_LAUNCH,
                        "Predicated index task launch for task %s "
                        "in parent task %s (UID %lld) has non-void "
                        "return type but no default value for its "
                        "future if the task predicate evaluates to "
                        "false.  Please set either the "
                        "'predicate_false_result' or "
                        "'predicate_false_future' fields of the "
                        "IndexTaskLauncher struct.", impl->get_name(), 
                        get_task_name(), get_unique_id())
        result->set_result(NULL);
      }
      return Future(result);
    }

    //--------------------------------------------------------------------------
    IndexSpace TaskContext::find_index_launch_space(const Domain &domain)
    //--------------------------------------------------------------------------
    {
      std::map<Domain,IndexSpace>::const_iterator finder =
        index_launch_spaces.find(domain);
      if (finder != index_launch_spaces.end())
        return finder->second;
      IndexSpace result;
      switch (domain.get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            result = TaskContext::create_index_space(domain, \
              NT_TemplateHelper::encode_tag<DIM,coord_t>()); \
            break; \
          }
        LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      index_launch_spaces[domain] = result;
      return result;
    }

    //--------------------------------------------------------------------------
    VariantImpl* TaskContext::select_inline_variant(TaskOp *child,
                              const std::vector<PhysicalRegion> &parent_regions,
                              std::deque<InstanceSet> &physical_instances)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, SELECT_INLINE_VARIANT_CALL);
      Mapper::SelectVariantInput input;
      Mapper::SelectVariantOutput output;
      input.processor = executing_processor;
      input.chosen_instances.resize(parent_regions.size());
      // Extract the specific field instances for each region requirement
      for (unsigned idx1 = 0; idx1 < parent_regions.size(); idx1++)
      {
        const RegionRequirement &child_req = child->regions[idx1];
        FieldSpaceNode *space = 
          runtime->forest->get_node(child_req.parent.get_field_space());
        FieldMask mask = space->get_field_mask(child_req.privilege_fields);
        InstanceSet instances;
        parent_regions[idx1].impl->get_references(instances);
        for (unsigned idx2 = 0; idx2 < instances.size(); idx2++)
        {
          const InstanceRef &ref = instances[idx2];
          const FieldMask overlap = mask & ref.get_valid_fields();
          if (!overlap)
            continue;
          physical_instances[idx1].add_instance(
              InstanceRef(ref.get_manager(), overlap, ref.get_ready_event()));
          input.chosen_instances[idx1].push_back(
              MappingInstance(ref.get_manager()));
          mask -= overlap;
          if (!mask)
            break;
        }
#ifdef DEBUG_LEGION
        assert(!mask);
#endif
      }
      output.chosen_variant = 0;
      // Always do this with the child mapper
      MapperManager *child_mapper = 
        runtime->find_mapper(executing_processor, child->map_id);
      child_mapper->invoke_select_task_variant(child, &input, &output);
      VariantImpl *variant_impl= runtime->find_variant_impl(child->task_id,
                                  output.chosen_variant, true/*can fail*/);
      if (variant_impl == NULL)
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from invoction of "
                      "'select_task_variant' on mapper %s. Mapper selected "
                      "an invalid variant ID %d for inlining of task %s "
                      "(UID %lld).", child_mapper->get_mapper_name(),
                      output.chosen_variant, child->get_task_name(), 
                      child->get_unique_id())
      if (!runtime->unsafe_mapper)
        child->validate_variant_selection(child_mapper, variant_impl,
         executing_processor.kind(), physical_instances, "select_task_variant");
      return variant_impl;
    }

    /////////////////////////////////////////////////////////////
    // Inner Context 
    /////////////////////////////////////////////////////////////
    
    //--------------------------------------------------------------------------
    InnerContext::InnerContext(Runtime *rt, SingleTask *owner,int d,bool finner,
                               const std::vector<RegionRequirement> &reqs,
                               const std::vector<RegionRequirement> &out_reqs,
                               const std::vector<unsigned> &parent_indexes,
                               const std::vector<bool> &virt_mapped,
                               UniqueID uid, ApEvent exec_fence, bool remote,
                               bool inline_task)
      : TaskContext(rt, owner, d, reqs, out_reqs, inline_task),
        tree_context(rt->allocate_region_tree_context()), context_uid(uid), 
        remote_context(remote), full_inner_context(finner),
        parent_req_indexes(parent_indexes), virtual_mapped(virt_mapped), 
        total_children_count(0), total_close_count(0), total_summary_count(0),
        outstanding_children_count(0), outstanding_prepipeline(0),
        outstanding_dependence(false),
        post_task_comp_queue(CompletionQueue::NO_QUEUE), 
        current_trace(NULL), previous_trace(NULL), valid_wait_event(false), 
        outstanding_subtasks(0), pending_subtasks(0), pending_frames(0), 
        currently_active_context(false), current_mapping_fence(NULL), 
        mapping_fence_gen(0), current_mapping_fence_index(0), 
        current_execution_fence_event(exec_fence),
        current_execution_fence_index(0), last_implicit(NULL),
        last_implicit_gen(0)
    //--------------------------------------------------------------------------
    {
      // Set some of the default values for a context
      context_configuration.max_window_size = 
        runtime->initial_task_window_size;
      context_configuration.hysteresis_percentage = 
        runtime->initial_task_window_hysteresis;
      context_configuration.max_outstanding_frames = 0;
      context_configuration.min_tasks_to_schedule = 
        runtime->initial_tasks_to_schedule;
      context_configuration.min_frames_to_schedule = 0;
      context_configuration.meta_task_vector_width = 
        runtime->initial_meta_task_vector_width;
      context_configuration.max_templates_per_trace =
        LEGION_DEFAULT_MAX_TEMPLATES_PER_TRACE;
      context_configuration.mutable_priority = false;
#ifdef DEBUG_LEGION
      assert(tree_context.exists());
      runtime->forest->check_context_state(tree_context);
#endif
      // If we have an owner, clone our local fields from its context
      // and also compute the coordinates for this context in the task tree
      if (owner != NULL)
      {
        TaskContext *owner_ctx = owner_task->get_context();
#ifdef DEBUG_LEGION
        InnerContext *parent_ctx = dynamic_cast<InnerContext*>(owner_ctx);
        assert(parent_ctx != NULL);
#else
        InnerContext *parent_ctx = static_cast<InnerContext*>(owner_ctx);
#endif
        parent_ctx->clone_local_fields(local_field_infos);
        // Get the coordinates for the parent task
        parent_ctx->compute_task_tree_coordinates(context_coordinates);
        // Then add our coordinates for our task
        context_coordinates.push_back(std::make_pair(
              owner_task->get_context_index(), owner_task->index_point));
      }
      if (!remote_context)
        runtime->register_local_context(context_uid, this);
    }

    //--------------------------------------------------------------------------
    InnerContext::InnerContext(const InnerContext &rhs)
      : TaskContext(NULL, NULL, 0, rhs.regions, rhs.output_reqs, false),
        tree_context(rhs.tree_context),
        context_uid(0), remote_context(false), full_inner_context(false),
        parent_req_indexes(rhs.parent_req_indexes), 
        virtual_mapped(rhs.virtual_mapped)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    InnerContext::~InnerContext(void)
    //--------------------------------------------------------------------------
    {
      if (!remote_instances.empty())
        free_remote_contexts();
      if (post_task_comp_queue.exists())
        post_task_comp_queue.destroy();
      for (std::map<TraceID,LegionTrace*>::const_iterator it = 
            traces.begin(); it != traces.end(); it++)
        if (it->second->remove_reference())
          delete (it->second);
      traces.clear();
      // Clean up any locks and barriers that the user
      // asked us to destroy
      while (!context_locks.empty())
      {
        context_locks.back().destroy_reservation();
        context_locks.pop_back();
      }
      while (!context_barriers.empty())
      {
        Realm::Barrier bar = context_barriers.back();
        bar.destroy_barrier();
        context_barriers.pop_back();
      }
      if (valid_wait_event)
      {
        valid_wait_event = false;
        Runtime::trigger_event(window_wait);
      }
      // No need for the lock here since we're being cleaned up
      if (!local_field_infos.empty())
        local_field_infos.clear();
      if (!fill_view_cache.empty())
      {
        for (std::list<FillView*>::const_iterator it = 
              fill_view_cache.begin(); it != fill_view_cache.end(); it++)
          if ((*it)->remove_base_valid_ref(CONTEXT_REF))
            delete (*it);
        fill_view_cache.clear();
      }
      while (!pending_equivalence_sets.empty())
      {
        LegionMap<RegionNode*,FieldMaskSet<PendingEquivalenceSet> >::aligned::
          iterator next = pending_equivalence_sets.begin();
        for (FieldMaskSet<PendingEquivalenceSet>::const_iterator it =
              next->second.begin(); it != next->second.end(); it++)
          if (it->first->finalize())
            delete it->first;
        pending_equivalence_sets.erase(next);
      }
#ifdef DEBUG_LEGION
      assert(pending_top_views.empty());
      assert(outstanding_subtasks == 0);
      assert(pending_subtasks == 0);
      assert(pending_frames == 0);
      assert(invalidated_refinements.empty());
#endif
      if (!remote_context)
        runtime->unregister_local_context(context_uid);
    }

    //--------------------------------------------------------------------------
    InnerContext& InnerContext::operator=(const InnerContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    void InnerContext::receive_resources(size_t return_index,
              std::map<LogicalRegion,unsigned> &created_regs,
              std::vector<LogicalRegion> &deleted_regs,
              std::set<std::pair<FieldSpace,FieldID> > &created_fids,
              std::vector<std::pair<FieldSpace,FieldID> > &deleted_fids,
              std::map<FieldSpace,unsigned> &created_fs,
              std::map<FieldSpace,std::set<LogicalRegion> > &latent_fs,
              std::vector<FieldSpace> &deleted_fs,
              std::map<IndexSpace,unsigned> &created_is,
              std::vector<std::pair<IndexSpace,bool> > &deleted_is,
              std::map<IndexPartition,unsigned> &created_partitions,
              std::vector<std::pair<IndexPartition,bool> > &deleted_partitions,
              std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      bool need_deletion_dependences = true;
      ApEvent precondition;
      std::map<Operation*,GenerationID> dependences;
      if (!created_regs.empty())
        register_region_creations(created_regs);
      if (!deleted_regs.empty())
      {
        precondition = 
          compute_return_deletion_dependences(return_index, dependences);
        need_deletion_dependences = false;
        register_region_deletions(precondition, dependences, 
                                  deleted_regs, preconditions);
      }
      if (!created_fids.empty())
        register_field_creations(created_fids);
      if (!deleted_fids.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_field_deletions(precondition, dependences, 
                                 deleted_fids, preconditions);
      }
      if (!created_fs.empty())
        register_field_space_creations(created_fs);
      if (!latent_fs.empty())
        register_latent_field_spaces(latent_fs);
      if (!deleted_fs.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_field_space_deletions(precondition, dependences,
                                       deleted_fs, preconditions);
      }
      if (!created_is.empty())
        register_index_space_creations(created_is);
      if (!deleted_is.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_index_space_deletions(precondition, dependences,
                                       deleted_is, preconditions);
      }
      if (!created_partitions.empty())
        register_index_partition_creations(created_partitions);
      if (!deleted_partitions.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_index_partition_deletions(precondition, dependences,
                                           deleted_partitions, preconditions);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_region_creations(
                                      std::map<LogicalRegion,unsigned> &regions)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (!latent_field_spaces.empty())
      {
        for (std::map<LogicalRegion,unsigned>::const_iterator it = 
              regions.begin(); it != regions.end(); it++)
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder =
            latent_field_spaces.find(it->first.get_field_space());
          if (finder != latent_field_spaces.end())
            finder->second.insert(it->first);
        }
      }
      if (!created_regions.empty())
      {
        for (std::map<LogicalRegion,unsigned>::const_iterator it = 
              regions.begin(); it != regions.end(); it++)
        {
          std::map<LogicalRegion,unsigned>::iterator finder = 
            created_regions.find(it->first);
          if (finder == created_regions.end())
          {
            created_regions.insert(*it);
            add_created_region(it->first, false/*task local*/);
          }
          else
            finder->second += it->second;
        }
      }
      else
      {
        created_regions.swap(regions);
        for (std::map<LogicalRegion,unsigned>::const_iterator it = 
              created_regions.begin(); it != created_regions.end(); it++)
          add_created_region(it->first, false/*task local*/);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_region_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                                            std::vector<LogicalRegion> &regions,
                                            std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      std::vector<LogicalRegion> delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<LogicalRegion>::const_iterator rit =
              regions.begin(); rit != regions.end(); rit++)
        {
          std::map<LogicalRegion,unsigned>::iterator region_finder = 
            created_regions.find(*rit);
          if (region_finder == created_regions.end())
          {
            if (local_regions.find(*rit) != local_regions.end())
              REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
                  "Local logical region (%x,%x,%x) in task %s (UID %lld) was "
                  "not deleted by this task. Local regions can only be deleted "
                  "by the task that made them.", rit->index_space.id,
                  rit->field_space.id, rit->tree_id, 
                  get_task_name(), get_unique_id())
            // Deletion keeps going up
            deleted_regions.push_back(*rit);
          }
          else
          {
            // One of ours to delete
#ifdef DEBUG_LEGION
            assert(region_finder->second > 0);
#endif
            if (--region_finder->second == 0)
            {
              // No need to delete this here, it will be deleted by the op
              delete_now.push_back(*rit);
              // Check to see if we have any latent field spaces to clean up
              if (!latent_field_spaces.empty())
              {
                std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder =
                  latent_field_spaces.find(rit->get_field_space());
                if (finder != latent_field_spaces.end())
                {
                  std::set<LogicalRegion>::iterator latent_finder = 
                    finder->second.find(*rit);
#ifdef DEBUG_LEGION
                  assert(latent_finder != finder->second.end());
#endif
                  finder->second.erase(latent_finder);
                  if (finder->second.empty())
                  {
                    // Now that all the regions using this field space have
                    // been deleted we can clean up all the created_fields
                    for (std::set<std::pair<FieldSpace,FieldID> >::iterator it =
                          created_fields.begin(); it != 
                          created_fields.end(); /*nothing*/)
                    {
                      if (it->first == finder->first)
                      {
                        std::set<std::pair<FieldSpace,FieldID> >::iterator 
                          to_delete = it++;
                        created_fields.erase(to_delete);
                      }
                      else
                        it++;
                    }
                    latent_field_spaces.erase(finder);
                  }
                }
              }
            }
          }
        }
      }
      if (!delete_now.empty())
      {
        for (std::vector<LogicalRegion>::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          DeletionOp *op = runtime->get_available_deletion_op();
          op->initialize_logical_region_deletion(this, *it, true/*unordered*/,
                                            true/*skip dependence analysis*/);
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_field_creations(
                               std::set<std::pair<FieldSpace,FieldID> > &fields)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (!created_fields.empty())
      {
#ifdef DEBUG_LEGION
        for (std::set<std::pair<FieldSpace,FieldID> >::const_iterator it = 
              fields.begin(); it != fields.end(); it++)
        {
          assert(created_fields.find(*it) == created_fields.end());
          created_fields.insert(*it);
        }
#else
        created_fields.insert(fields.begin(), fields.end());
#endif
      }
      else
        created_fields.swap(fields);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_field_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                           std::vector<std::pair<FieldSpace,FieldID> > &fields,
                           std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      std::map<FieldSpace,std::set<FieldID> > delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<FieldSpace,FieldID> >::const_iterator fit =
              fields.begin(); fit != fields.end(); fit++)
        {
          std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
            field_finder = created_fields.find(*fit);
          if (field_finder == created_fields.end())
          {
            std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
              local_finder = local_fields.find(*fit);
            if (local_finder != local_fields.end())
              REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
                  "Local field %d in field space %x in task %s (UID %lld) was "
                  "not deleted by this task. Local fields can only be deleted "
                  "by the task that made them.", fit->second, fit->first.id,
                  get_task_name(), get_unique_id())
            deleted_fields.push_back(*fit);
          }
          else
          {
            // One of ours to delete
            delete_now[fit->first].insert(fit->second);
            // No need to delete this now, it will be deleted
            // when the deletion op makes its region requirements
          }
        }
      }
      if (!delete_now.empty())
      {
        for (std::map<FieldSpace,std::set<FieldID> >::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          DeletionOp *op = runtime->get_available_deletion_op();
          FieldAllocatorImpl *allocator = 
            create_field_allocator(it->first, true/*unordered*/);
          op->initialize_field_deletions(this, it->first, it->second, 
             true/*unordered*/, allocator, false/*non owner shard*/,
             true/*skip dependence analysis*/);
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_field_space_creations(
                                          std::map<FieldSpace,unsigned> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (!latent_field_spaces.empty())
      {
        // Remove any latent field spaces we have ownership for
        for (std::map<FieldSpace,unsigned>::const_iterator it =
              spaces.begin(); it != spaces.end(); it++)
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder = 
            latent_field_spaces.find(it->first);
          if (finder != latent_field_spaces.end())
            latent_field_spaces.erase(finder);
        }
      }
      if (!created_field_spaces.empty())
      {
        for (std::map<FieldSpace,unsigned>::const_iterator it = 
              spaces.begin(); it != spaces.end(); it++)
        {
          std::map<FieldSpace,unsigned>::iterator finder = 
            created_field_spaces.find(it->first);
          if (finder == created_field_spaces.end())
            created_field_spaces.insert(*it);
          else
            finder->second += it->second;
        }
      }
      else
        created_field_spaces.swap(spaces);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_latent_field_spaces(
                          std::map<FieldSpace,std::set<LogicalRegion> > &spaces)
    //--------------------------------------------------------------------------
    {
      AutoLock p_lock(privilege_lock);
      if (!created_field_spaces.empty())
      {
        // Remote any latent field spaces we already have ownership on
        for (std::map<FieldSpace,std::set<LogicalRegion> >::iterator it =
              spaces.begin(); it != spaces.end(); /*nothing*/)
        {
          if (created_field_spaces.find(it->first) != 
                created_field_spaces.end())
          {
            std::map<FieldSpace,std::set<LogicalRegion> >::iterator 
              to_delete = it++;
            spaces.erase(to_delete);
          }
          else
            it++;
        }
        if (spaces.empty())
          return;
      }
      if (!created_regions.empty())
      {
        // See if any of these regions are copies of our latent spaces
        for (std::map<LogicalRegion,unsigned>::const_iterator it = 
              created_regions.begin(); it != created_regions.end(); it++)
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder = 
            spaces.find(it->first.get_field_space());
          if (finder != spaces.end())
            finder->second.insert(it->first);
        }
      }
      // Now we can do the merge
      if (!latent_field_spaces.empty())
      {
        for (std::map<FieldSpace,std::set<LogicalRegion> >::const_iterator it =
              spaces.begin(); it != spaces.end(); it++)
        {
          std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder = 
            latent_field_spaces.find(it->first);
          if (finder != latent_field_spaces.end())
            finder->second.insert(it->second.begin(), it->second.end());
          else
            latent_field_spaces.insert(*it);
        }
      }
      else
        latent_field_spaces.swap(spaces);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_field_space_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                                               std::vector<FieldSpace> &spaces,
                                               std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      std::vector<FieldSpace> delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<FieldSpace>::const_iterator fit = 
              spaces.begin(); fit != spaces.end(); fit++)
        {
          std::map<FieldSpace,unsigned>::iterator finder = 
            created_field_spaces.find(*fit);
          if (finder != created_field_spaces.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(*fit);
              created_field_spaces.erase(finder);
              // Count how many regions are still using this field space
              // that still need to be deleted before we can remove the
              // list of created fields
              std::set<LogicalRegion> remaining_regions;
              for (std::map<LogicalRegion,unsigned>::const_iterator it = 
                    created_regions.begin(); it != created_regions.end(); it++)
                if (it->first.get_field_space() == *fit)
                  remaining_regions.insert(it->first);
              for (std::map<LogicalRegion,bool>::const_iterator it = 
                    local_regions.begin(); it != local_regions.end(); it++)
                if (it->first.get_field_space() == *fit)
                  remaining_regions.insert(it->first);
              if (remaining_regions.empty())
              {
                // No remaining regions so we can remove any created fields now
                for (std::set<std::pair<FieldSpace,FieldID> >::iterator it = 
                      created_fields.begin(); it != 
                      created_fields.end(); /*nothing*/)
                {
                  if (it->first == *fit)
                  {
                    std::set<std::pair<FieldSpace,FieldID> >::iterator 
                      to_delete = it++;
                    created_fields.erase(to_delete);
                  }
                  else
                    it++;
                }
              }
              else
                latent_field_spaces[*fit] = remaining_regions;
            }
          }
          else
            // If we didn't make this field space, record the deletion
            // and keep going. It will be handled by the context that
            // made the field space
            deleted_field_spaces.push_back(*fit);
        }
      }
      if (!delete_now.empty())
      {
        for (std::vector<FieldSpace>::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          DeletionOp *op = runtime->get_available_deletion_op();
          op->initialize_field_space_deletion(this, *it, true/*unordered*/);
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_index_space_creations(
                                          std::map<IndexSpace,unsigned> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (!created_index_spaces.empty())
      {
        for (std::map<IndexSpace,unsigned>::const_iterator it = 
              spaces.begin(); it != spaces.end(); it++)
        {
          std::map<IndexSpace,unsigned>::iterator finder = 
            created_index_spaces.find(it->first);
          if (finder == created_index_spaces.end())
            created_index_spaces.insert(*it);
          else
            finder->second += it->second;
        }
      }
      else
        created_index_spaces.swap(spaces);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_index_space_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                               std::vector<std::pair<IndexSpace,bool> > &spaces,
                                               std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      std::vector<IndexSpace> delete_now;
      std::vector<std::vector<IndexPartition> > sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<IndexSpace,bool> >::const_iterator sit =
              spaces.begin(); sit != spaces.end(); sit++)
        {
          std::map<IndexSpace,unsigned>::iterator finder = 
            created_index_spaces.find(sit->first);
          if (finder != created_index_spaces.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(sit->first);
              sub_partitions.resize(sub_partitions.size() + 1);
              created_index_spaces.erase(finder);
              if (sit->second)
              {
                std::vector<IndexPartition> &subs = sub_partitions.back();
                // Also remove any index partitions for this index space tree
                for (std::map<IndexPartition,unsigned>::iterator it = 
                      created_index_partitions.begin(); it !=
                      created_index_partitions.end(); /*nothing*/)
                {
                  if (it->first.get_tree_id() == sit->first.get_tree_id()) 
                  {
#ifdef DEBUG_LEGION
                    assert(it->second > 0);
#endif
                    if (--it->second == 0)
                    {
                      subs.push_back(it->first);
                      std::map<IndexPartition,unsigned>::iterator 
                        to_delete = it++;
                      created_index_partitions.erase(to_delete);
                    }
                    else
                      it++;
                  }
                  else
                    it++;
                }
              }
            }
          }
          else
            // If we didn't make the index space in this context, just
            // record it and keep going, it will get handled later
            deleted_index_spaces.push_back(*sit);
        }
      }
      if (!delete_now.empty())
      {
#ifdef DEBUG_LEGION
        assert(delete_now.size() == sub_partitions.size());
#endif
        for (unsigned idx = 0; idx < delete_now.size(); idx++)
        {
          DeletionOp *op = runtime->get_available_deletion_op();
          op->initialize_index_space_deletion(this, delete_now[idx], 
                            sub_partitions[idx], true/*unordered*/);
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_index_partition_creations(
                                       std::map<IndexPartition,unsigned> &parts)
    //--------------------------------------------------------------------------
    {
      AutoLock priv_lock(privilege_lock);
      if (!created_index_partitions.empty())
      {
        for (std::map<IndexPartition,unsigned>::const_iterator it = 
              parts.begin(); it != parts.end(); it++)
        {
          std::map<IndexPartition,unsigned>::iterator finder = 
            created_index_partitions.find(it->first);
          if (finder == created_index_partitions.end())
            created_index_partitions.insert(*it);
          else
            finder->second += it->second;
        }
      }
      else
        created_index_partitions.swap(parts);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_index_partition_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                            std::vector<std::pair<IndexPartition,bool> > &parts, 
                                               std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      std::vector<IndexPartition> delete_now;
      std::vector<std::vector<IndexPartition> > sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<IndexPartition,bool> >::const_iterator pit =
              parts.begin(); pit != parts.end(); pit++)
        {
          std::map<IndexPartition,unsigned>::iterator finder = 
            created_index_partitions.find(pit->first);
          if (finder != created_index_partitions.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(pit->first);
              sub_partitions.resize(sub_partitions.size() + 1);
              created_index_partitions.erase(finder);
              if (pit->second)
              {
                std::vector<IndexPartition> &subs = sub_partitions.back();
                // Remove any other partitions that this partition dominates
                for (std::map<IndexPartition,unsigned>::iterator it = 
                      created_index_partitions.begin(); it !=
                      created_index_partitions.end(); /*nothing*/)
                {
                  if ((pit->first.get_tree_id() == it->first.get_tree_id()) &&
                      runtime->forest->is_dominated_tree_only(it->first, 
                                                              pit->first))
                  {
#ifdef DEBUG_LEGION
                    assert(it->second > 0);
#endif
                    if (--it->second == 0)
                    {
                      subs.push_back(it->first);
                      std::map<IndexPartition,unsigned>::iterator 
                        to_delete = it++;
                      created_index_partitions.erase(to_delete);
                    }
                    else
                      it++;
                  }
                  else
                    it++;
                }
              }
            }
          }
          else
            // If we didn't make the partition, record it and keep going
            deleted_index_partitions.push_back(*pit);
        }
      }
      if (!delete_now.empty())
      {
#ifdef DEBUG_LEGION
        assert(delete_now.size() == sub_partitions.size());
#endif
        for (unsigned idx = 0; idx < delete_now.size(); idx++)
        {
          DeletionOp *op = runtime->get_available_deletion_op();
          op->initialize_index_part_deletion(this, delete_now[idx], 
                            sub_partitions[idx], true/*unordered*/);
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    ApEvent InnerContext::compute_return_deletion_dependences(
            size_t return_index, std::map<Operation*,GenerationID> &dependences)
    //--------------------------------------------------------------------------
    {
      // This is a mixed mapping and execution fence analysis 
      std::set<ApEvent> previous_events;
      {
        AutoLock child_lock(child_op_lock,1,false/*exclusive*/); 
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executing_children.begin(); it != executing_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our deletion we don't care
          if (op_index >= return_index)
            continue;
          dependences.insert(*it);
          previous_events.insert(it->first->get_completion_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executed_children.begin(); it != executed_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our deletion we don't care
          if (op_index >= return_index)
            continue;
          dependences.insert(*it);
          previous_events.insert(it->first->get_completion_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              complete_children.begin(); it != complete_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our deletion we don't care
          if (op_index >= return_index)
            continue;
          dependences.insert(*it);
          previous_events.insert(it->first->get_completion_event());
        }
      }
      // Do not check the current execution fence as it may have come after us
      if (!previous_events.empty())
        return Runtime::merge_events(NULL, previous_events);
      return ApEvent::NO_AP_EVENT;
    }

    //--------------------------------------------------------------------------
    RegionTreeContext InnerContext::get_context(void) const
    //--------------------------------------------------------------------------
    {
      return tree_context;
    }

    //--------------------------------------------------------------------------
    ContextID InnerContext::get_context_id(void) const
    //--------------------------------------------------------------------------
    {
      return tree_context.get_id();
    }

    //--------------------------------------------------------------------------
    UniqueID InnerContext::get_context_uid(void) const
    //--------------------------------------------------------------------------
    {
      return context_uid;
    }

    //--------------------------------------------------------------------------
    bool InnerContext::is_inner_context(void) const
    //--------------------------------------------------------------------------
    {
      return full_inner_context;
    }

    //--------------------------------------------------------------------------
    RtEvent InnerContext::compute_equivalence_sets(EqSetTracker *target,
                               AddressSpaceID target_space, RegionNode *region,
                               const FieldMask &mask, const UniqueID opid,
                               const AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      // We know we are on a node now where the version information
      // is up to date so we can call into the region tree to actually
      // compute the equivalence sets for this expression
      std::set<RtEvent> ready;
      const ContextID ctx = get_context_id();
      region->compute_equivalence_sets(ctx, this, target, target_space,
                                       region->row_source, mask, opid, source, 
                                       ready, false/*down only*/);
      if (!ready.empty())
        return Runtime::merge_events(ready);
      else
        return RtEvent::NO_RT_EVENT;
    } 

    //--------------------------------------------------------------------------
    void InnerContext::record_pending_disjoint_complete_set(
                              PendingEquivalenceSet *set, const FieldMask &mask)
    //--------------------------------------------------------------------------
    {
      AutoLock p_lock(pending_set_lock);
      FieldMaskSet<PendingEquivalenceSet> &pending_sets =
        pending_equivalence_sets[set->region_node];
      if (!(mask * pending_sets.get_valid_mask()))
      {
#ifdef DEBUG_LEGION
        // This should only happen for empty index spaces when there is
        // nothing else that will prune them out prior to this
        assert(set->region_node->row_source->is_empty());
#endif
        std::vector<PendingEquivalenceSet*> to_delete;
        for (FieldMaskSet<PendingEquivalenceSet>::iterator it =
              pending_sets.begin(); it != pending_sets.end(); it++)
        {
          it.filter(mask);
          if (!it->second)
            to_delete.push_back(it->first);
        }
        for (std::vector<PendingEquivalenceSet*>::const_iterator it =
              to_delete.begin(); it != to_delete.end(); it++)
        {
          pending_sets.erase(*it);
          if ((*it)->finalize())
            delete (*it);
        }
      }
      pending_sets.insert(set, mask);
    }

    //--------------------------------------------------------------------------
    bool InnerContext::finalize_disjoint_complete_sets(RegionNode *region, 
            VersionManager *target, FieldMask request_mask, const UniqueID opid,
            const AddressSpaceID source, RtUserEvent ready_event)
    //--------------------------------------------------------------------------
    {
      std::set<RtEvent> applied_events;
      AutoLock p_lock(pending_set_lock);
      LegionMap<RegionNode*,FieldMaskSet<PendingEquivalenceSet> >::aligned
        ::iterator finder = pending_equivalence_sets.find(region);
      if (finder == pending_equivalence_sets.end())
      {
#ifdef DEBUG_LEGION
        assert(region->row_source->is_empty());
#endif
        Runtime::trigger_event(ready_event);
        return true;
      }
      std::vector<PendingEquivalenceSet*> to_delete;
      for (FieldMaskSet<PendingEquivalenceSet>::iterator it =
            finder->second.begin(); it != finder->second.end(); it++) 
      {
        const FieldMask overlap = request_mask & it->second;
        if (!overlap)
          continue;
        EquivalenceSet *new_set = 
          it->first->compute_refinement(source, runtime, applied_events);
        FieldMask dummy_parent;
        target->record_refinement(new_set,overlap,dummy_parent,applied_events);
        it.filter(overlap);
        // Filter the valid mask too
        finder->second.filter_valid_mask(overlap);
        if (!it->second)
          to_delete.push_back(it->first);
        request_mask -= overlap;
        if (!request_mask)
          break;
      }
#ifdef DEBUG_LEGION
      assert(!request_mask); // should have seen all the fields
#endif
      if (!to_delete.empty())
      {
        if (to_delete.size() != finder->second.size())
        {
          for (std::vector<PendingEquivalenceSet*>::const_iterator it =
                to_delete.begin(); it != to_delete.end(); it++)
          {
            finder->second.erase(*it);
            if ((*it)->finalize())
              delete (*it);
          }
        }
        else
        {
          for (std::vector<PendingEquivalenceSet*>::const_iterator it =
                to_delete.begin(); it != to_delete.end(); it++)
            if ((*it)->finalize())
              delete (*it);
          pending_equivalence_sets.erase(finder);
        }
      }
      // We're done now so trigger the ready event and tell the caller too
      if (!applied_events.empty())
        Runtime::trigger_event(ready_event, 
            Runtime::merge_events(applied_events));
      else
        Runtime::trigger_event(ready_event);
      return true;
    }

    //--------------------------------------------------------------------------
    void InnerContext::invalidate_disjoint_complete_sets(RegionNode *region,
                                                         const FieldMask &mask)
    //--------------------------------------------------------------------------
    {
      AutoLock p_lock(pending_set_lock);
      LegionMap<RegionNode*,FieldMaskSet<PendingEquivalenceSet> >::aligned
        ::iterator finder = pending_equivalence_sets.find(region);
      if ((finder == pending_equivalence_sets.end()) ||
          (finder->second.get_valid_mask() * mask))
        return;
      std::vector<PendingEquivalenceSet*> to_delete;
      for (FieldMaskSet<PendingEquivalenceSet>::iterator it =
            finder->second.begin(); it != finder->second.end(); it++)
      {
        it.filter(mask);
        if (!it->second)
          to_delete.push_back(it->first);
      }
#ifdef DEBUG_LEGION
      assert(!to_delete.empty());
#endif
      if (to_delete.size() != finder->second.size())
      {
        for (std::vector<PendingEquivalenceSet*>::const_iterator it =
              to_delete.begin(); it != to_delete.end(); it++)
        {
          finder->second.erase(*it);
          if ((*it)->finalize())
            delete (*it);
        }
        finder->second.filter_valid_mask(mask);
      }
      else
      {
        for (std::vector<PendingEquivalenceSet*>::const_iterator it =
              to_delete.begin(); it != to_delete.end(); it++)
          if ((*it)->finalize())
            delete (*it);
        pending_equivalence_sets.erase(finder);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::deduplicate_invalidate_trackers(
                                 const FieldMaskSet<EquivalenceSet> &to_untrack,
                                 std::set<RtEvent> &applied_events)
    //--------------------------------------------------------------------------
    {
      for (FieldMaskSet<EquivalenceSet>::const_iterator it =
            to_untrack.begin(); it != to_untrack.end(); it++)
        it->first->invalidate_trackers(it->second, applied_events,
            runtime->address_space, NULL/*no collective mapping*/);
    }

    //--------------------------------------------------------------------------
    InnerContext* InnerContext::find_parent_physical_context(unsigned index)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(regions.size() == virtual_mapped.size());
      assert(regions.size() == parent_req_indexes.size());
#endif     
      if (index < virtual_mapped.size())
      {
        // See if it is virtual mapped
        if (virtual_mapped[index])
          return find_parent_context()->find_parent_physical_context(
                                            parent_req_indexes[index]);
        else // We mapped a physical instance so we're it
          return this;
      }
      else // We created it
      {
        // Check to see if this has returnable privileges or not
        // If they are not returnable, then we can just be the 
        // context for the handling the meta-data management, 
        // otherwise if they are returnable then the top-level
        // context has to provide global guidance about which
        // node manages the meta-data.
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<unsigned,bool>::const_iterator finder = 
          returnable_privileges.find(index);
        if ((finder != returnable_privileges.end()) && !finder->second)
          return this;
      }
      return find_top_context();
    }

    //--------------------------------------------------------------------------
    InnerContext* InnerContext::find_top_context(InnerContext *previous)
    //--------------------------------------------------------------------------
    {
      TaskContext *parent = find_parent_context();
      if (parent != NULL)
        return parent->find_top_context(this);
#ifdef DEBUG_LEGION
      assert(previous != NULL);
#endif
      return previous;
    }

    //--------------------------------------------------------------------------
    void InnerContext::pack_remote_context(Serializer &rez, 
                                           AddressSpaceID target,bool replicate)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, PACK_REMOTE_CONTEXT_CALL);
#ifdef DEBUG_LEGION
      assert(owner_task != NULL);
#endif
      rez.serialize(depth);
      // See if we need to pack up base task information
      owner_task->pack_external_task(rez, target);
#ifdef DEBUG_LEGION
      assert(regions.size() == parent_req_indexes.size());
#endif
      for (unsigned idx = 0; idx < regions.size(); idx++)
        rez.serialize(parent_req_indexes[idx]);
      // Pack up our virtual mapping information
      std::vector<unsigned> virtual_indexes;
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        if (virtual_mapped[idx])
          virtual_indexes.push_back(idx);
      }
      rez.serialize<size_t>(virtual_indexes.size());
      for (unsigned idx = 0; idx < virtual_indexes.size(); idx++)
        rez.serialize(virtual_indexes[idx]);
      rez.serialize(find_parent_context()->get_context_uid());
      rez.serialize<size_t>(context_coordinates.size());
      for (std::vector<std::pair<size_t,DomainPoint> >::const_iterator it =
            context_coordinates.begin(); it != context_coordinates.end(); it++)
      {
        rez.serialize(it->first);
        rez.serialize(it->second);
      }
      // Finally pack the local field infos
      AutoLock local_lock(local_field_lock,1,false/*exclusive*/);
      rez.serialize<size_t>(local_field_infos.size());
      for (std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator 
            it = local_field_infos.begin(); 
            it != local_field_infos.end(); it++)
      {
        rez.serialize(it->first);
        rez.serialize<size_t>(it->second.size());
        for (unsigned idx = 0; idx < it->second.size(); idx++)
          rez.serialize(it->second[idx]);
      }
      rez.serialize<bool>(replicate);
    }

    //--------------------------------------------------------------------------
    void InnerContext::unpack_remote_context(Deserializer &derez,
                                           std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      assert(false); // should only be called for RemoteContext
    }

    //--------------------------------------------------------------------------
    void InnerContext::compute_task_tree_coordinates(
                       std::vector<std::pair<size_t,DomainPoint> > &coordinates)
    //--------------------------------------------------------------------------
    {
      coordinates = context_coordinates;
    } 

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space(const Future &future, 
                                                TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      IndexSpace handle(runtime->get_unique_index_space_id(),
                        runtime->get_unique_index_tree_id(), type_tag);
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space %x in task%s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id()); 
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_index_space(handle.id);
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();
      const ApEvent ready = creator_op->get_completion_event();
      IndexSpaceNode *node = runtime->forest->create_index_space(handle, 
          NULL/*domain*/, did, true/*notify remote*/, 0/*expr id*/, ready);
      creator_op->initialize_index_space(this, node, future);
      register_index_space_creation(handle);
      add_to_dependence_queue(creator_op);
      return handle;
    } 

    //--------------------------------------------------------------------------
    void InnerContext::destroy_index_space(IndexSpace handle, 
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      log_index.debug("Destroying index space %x in task %s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id());
#endif
      // Check to see if this is a top-level index space, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_index_space(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy index space %x in task %s (UID %lld) "
            "which is not a top-level index space. Legion only permits "
            "top-level index spaces to be destroyed.", handle.get_id(),
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      std::vector<IndexPartition> sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexSpace,unsigned>::iterator finder = 
          created_index_spaces.find(handle);
        if (finder == created_index_spaces.end())
        {
          // If we didn't make the index space in this context, just
          // record it and keep going, it will get handled later
          deleted_index_spaces.push_back(std::make_pair(handle,recurse));
          return;
        }
        else
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_index_spaces.erase(finder);
          else
            return;
        }
        if (recurse)
        {
          // Also remove any index partitions for this index space tree
          for (std::map<IndexPartition,unsigned>::iterator it = 
                created_index_partitions.begin(); it !=
                created_index_partitions.end(); /*nothing*/)
          {
            if (it->first.get_tree_id() == handle.get_tree_id()) 
            {
              sub_partitions.push_back(it->first);
#ifdef DEBUG_LEGION
              assert(it->second > 0);
#endif
              if (--it->second == 0)
              {
                std::map<IndexPartition,unsigned>::iterator to_delete = it++;
                created_index_partitions.erase(to_delete);
              }
              else
                it++;
            }
            else
              it++;
          }
        }
      }
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_index_space_deletion(this,handle,sub_partitions,unordered);
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void InnerContext::destroy_index_partition(IndexPartition handle,
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      log_index.debug("Destroying index partition %x in task %s (ID %lld)",
                      handle.id, get_task_name(), get_unique_id());
#endif
      std::vector<IndexPartition> sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexPartition,unsigned>::iterator finder = 
          created_index_partitions.find(handle);
        if (finder != created_index_partitions.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_index_partitions.erase(finder);
          else
            return;
          if (recurse)
          {
            // Remove any other partitions that this partition dominates
            for (std::map<IndexPartition,unsigned>::iterator it = 
                  created_index_partitions.begin(); it !=
                  created_index_partitions.end(); /*nothing*/)
            {
              if ((handle.get_tree_id() == it->first.get_tree_id()) &&
                  runtime->forest->is_dominated_tree_only(it->first, handle))
              {
                sub_partitions.push_back(it->first);
#ifdef DEBUG_LEGION
                assert(it->second > 0);
#endif
                if (--it->second == 0)
                {
                  std::map<IndexPartition,unsigned>::iterator to_delete = it++;
                  created_index_partitions.erase(to_delete);
                }
                else
                  it++;
              }
              else
                it++;
            }
          }
        }
        else
        {
          // If we didn't make the partition, record it and keep going
          deleted_index_partitions.push_back(std::make_pair(handle,recurse));
          return;
        }
      }
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_index_part_deletion(this, handle, 
                                         sub_partitions, unordered);
      add_to_dependence_queue(op, unordered);
    }
    
    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_equal_partition(
                                                      IndexSpace parent,
                                                      IndexSpace color_space,
                                                      size_t granularity,
                                                      Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating equal partition %d with parent index space %x "
                      "in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_equal_partition(this, pid, granularity);
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this,pid,parent,
                    color_space, partition_color, LEGION_DISJOINT_COMPLETE_KIND,
                    did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_weights(IndexSpace parent,
                                                const FutureMap &weights, 
                                                IndexSpace color_space,
                                                size_t granularity, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      const IndexPartition pid(runtime->get_unique_index_partition_id(), 
                               parent.get_tree_id(), parent.get_type_tag());
      const DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition %d by weights with parent index "
                      "space %x in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_weight_partition(this, pid, weights, granularity);
      const ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RegionTreeForest *forest = runtime->forest;
      const RtEvent safe = forest->create_pending_partition(this, pid, parent,
                  color_space, partition_color, LEGION_DISJOINT_COMPLETE_KIND,
                  did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_union(
                                          IndexSpace parent,
                                          IndexPartition handle1,
                                          IndexPartition handle2,
                                          IndexSpace color_space,
                                          PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating union partition %d with parent index "
                      "space %x in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create "
                        "partition by union!", handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create "
                        "partition by union!", handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_union_partition(this, pid, handle1, handle2);
      ApEvent term_event = part_op->get_completion_event();
      // If either partition is aliased the result is aliased
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        // If one of these partitions is aliased then the result is aliased
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (p1->is_disjoint(true/*from app*/))
        {
          IndexPartNode *p2 = runtime->forest->get_node(handle2);
          if (!p2->is_disjoint(true/*from app*/))
          {
            if (kind == LEGION_COMPUTE_KIND)
              kind = LEGION_ALIASED_KIND;
            else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
              kind = LEGION_ALIASED_COMPLETE_KIND;
            else
              kind = LEGION_ALIASED_INCOMPLETE_KIND;
          }
        }
        else
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_ALIASED_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_ALIASED_COMPLETE_KIND;
          else
            kind = LEGION_ALIASED_INCOMPLETE_KIND;
        }
      }
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, partition_color, kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__); 
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_intersection(
                                              IndexSpace parent,
                                              IndexPartition handle1,
                                              IndexPartition handle2,
                                              IndexSpace color_space,
                                              PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating intersection partition %d with parent "
                      "index space %x in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create partition by "
                        "intersection!", handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create partition by "
                        "intersection!", handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_intersection_partition(this, pid, handle1, handle2);
      ApEvent term_event = part_op->get_completion_event();
      // If either partition is disjoint then the result is disjoint
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (!p1->is_disjoint(true/*from app*/))
        {
          IndexPartNode *p2 = runtime->forest->get_node(handle2);
          if (p2->is_disjoint(true/*from app*/))
          {
            if (kind == LEGION_COMPUTE_KIND)
              kind = LEGION_DISJOINT_KIND;
            else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
              kind = LEGION_DISJOINT_COMPLETE_KIND;
            else
              kind = LEGION_DISJOINT_INCOMPLETE_KIND;
          }
        }
        else
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, partition_color, kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_intersection(
                                              IndexSpace parent,
                                              IndexPartition partition,
                                              PartitionKind kind, Color color,
                                              bool dominates)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating intersection partition %d with parent "
                      "index space %x in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
      if (parent.get_type_tag() != partition.get_type_tag())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
            "IndexPartition %d does not have the same type as the "
            "parent index space %x in task %s (UID %lld)", partition.id,
            parent.id, get_task_name(), get_unique_id())
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_intersection_partition(this,pid,partition,dominates);
      ApEvent term_event = part_op->get_completion_event();
      IndexPartNode *part_node = runtime->forest->get_node(partition);
      // See if we can determine disjointness if we weren't told
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        if (part_node->is_disjoint(true/*from app*/))
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid,parent,
        part_node->color_space->handle, partition_color, kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_difference(
                                                  IndexSpace parent,
                                                  IndexPartition handle1,
                                                  IndexPartition handle2,
                                                  IndexSpace color_space,
                                                  PartitionKind kind, 
                                                  Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating difference partition %d with parent "
                      "index space %x in task %s (ID %lld)", pid.id, parent.id,
                      get_task_name(), get_unique_id());
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                              "index tree as IndexSpace %d in create "
                              "partition by difference!",
                              handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                              "index tree as IndexSpace %d in create "
                              "partition by difference!",
                              handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_difference_partition(this, pid, handle1, handle2);
      ApEvent term_event = part_op->get_completion_event();
      // If the left-hand-side is disjoint the result is disjoint
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (p1->is_disjoint(true/*from app*/))
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, partition_color, kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    Color InnerContext::create_cross_product_partitions(
                                                      IndexPartition handle1,
                                                      IndexPartition handle2,
                                   std::map<IndexSpace,IndexPartition> &handles,
                                                      PartitionKind kind,
                                                      Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating cross product partitions in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
      if (handle1.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
          "IndexPartition %d is not part of the same "
                              "index tree as IndexPartition %d in create "
                              "cross product partitions!",
                              handle1.id, handle2.id)
#endif
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      std::set<RtEvent> safe_events;
      runtime->forest->create_pending_cross_product(this, handle1, handle2, 
                  handles, kind, partition_color, term_event, safe_events);
      part_op->initialize_cross_product(this, handle1, handle2,partition_color);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      if (!safe_events.empty())
      {
        const RtEvent wait_on = Runtime::merge_events(safe_events);
        if (wait_on.exists() && !wait_on.has_triggered())
          wait_on.wait();
      }
      if (runtime->verify_partitions)
      {
        Domain color_space = runtime->get_index_partition_color_space(handle1);
        // This code will only work if the color space has type coord_t
        TypeTag type_tag;
        switch (color_space.get_dim())
        {
#define DIMFUNC(DIM) \
          case DIM: \
            { \
              type_tag = NT_TemplateHelper::encode_tag<DIM,coord_t>(); \
              assert(handle1.get_type_tag() == type_tag); \
              break; \
            }
          LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
          default:
            assert(false);
        }
        for (Domain::DomainPointIterator itr(color_space); itr; itr++)
        {
          IndexSpace subspace;
          switch (color_space.get_dim())
          {
#define DIMFUNC(DIM) \
            case DIM: \
              { \
                const Point<DIM,coord_t> p(itr.p); \
                subspace = runtime->get_index_subspace(handle1, &p, type_tag); \
                break; \
              }
            LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
            default:
              assert(false);
          }
          IndexPartition part = 
            runtime->get_index_partition(subspace, partition_color);
          verify_partition(part, verify_kind, __func__);
        }
      }
      return partition_color;
    }

    //--------------------------------------------------------------------------
    void InnerContext::create_association(LogicalRegion domain,
                                          LogicalRegion domain_parent,
                                          FieldID domain_fid,
                                          IndexSpace range,
                                          MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating association in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op();
      part_op->initialize_by_association(this, domain, domain_parent, 
                                         domain_fid, range, id, tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_association call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_restricted_partition(
                                              IndexSpace parent,
                                              IndexSpace color_space,
                                              const void *transform,
                                              size_t transform_size,
                                              const void *extent,
                                              size_t extent_size,
                                              PartitionKind part_kind,
                                              Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating restricted partition in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color; 
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_restricted_partition(this, pid, transform, 
                                transform_size, extent, extent_size);
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, part_color, part_kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_domain(
                                                IndexSpace parent,
                                    const std::map<DomainPoint,Domain> &domains,
                                                IndexSpace color_space,
                                                bool perform_intersections,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      ArgumentMap argmap;
      for (std::map<DomainPoint,Domain>::const_iterator it = 
            domains.begin(); it != domains.end(); it++)
        argmap.set_point(it->first,
            TaskArgument(&it->second, sizeof(it->second)));
      FutureMap future_map(argmap.impl->freeze(this));
      return create_partition_by_domain(parent, future_map, color_space, 
                                        perform_intersections, part_kind,color);
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_domain(
                                                IndexSpace parent,
                                                const FutureMap &domains,
                                                IndexSpace color_space,
                                                bool perform_intersections,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by domain in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color; 
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      part_op->initialize_by_domain(this, pid, domains, perform_intersections);
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, part_color, part_kind, did, term_event);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_field(
                                              LogicalRegion handle,
                                              LogicalRegion parent_priv,
                                              FieldID fid,
                                              IndexSpace color_space,
                                              Color color,
                                              MapperID id, MappingTagID tag,
                                              PartitionKind part_kind)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Partition by field is disjoint by construction
      PartitionKind verify_kind = LEGION_DISJOINT_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexSpace parent = handle.get_index_space(); 
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by field in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      // Allocate the partition operation
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition 
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            parent, color_space, part_color, part_kind, did, term_event);
      // Do this after creating the pending partition so the node exists
      // in case we need to look at it during initialization
      part_op->initialize_by_field(this, pid, handle, parent_priv, fid, id,tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_partition_by_field call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_image(
                                                    IndexSpace handle,
                                                    LogicalPartition projection,
                                                    LogicalRegion parent,
                                                    FieldID fid,
                                                    IndexSpace color_space,
                                                    PartitionKind part_kind,
                                                    Color color,
                                                    MapperID id, 
                                                    MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         handle.get_tree_id(), handle.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by image in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      // Allocate the partition operation
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op();
      ApEvent term_event = part_op->get_completion_event(); 
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            handle, color_space, part_color, part_kind, did, term_event);
      // Do this after creating the pending partition so the node exists
      // in case we need to look at it during initialization
      part_op->initialize_by_image(this, pid, projection, parent, fid, id, tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_partition_by_image call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_image_range(
                                                    IndexSpace handle,
                                                    LogicalPartition projection,
                                                    LogicalRegion parent,
                                                    FieldID fid,
                                                    IndexSpace color_space,
                                                    PartitionKind part_kind,
                                                    Color color,
                                                    MapperID id, 
                                                    MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         handle.get_tree_id(), handle.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by image range in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      // Allocate the partition operation
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
            handle, color_space, part_color, part_kind, did, term_event);
      // Do this after creating the pending partition so the node exists
      // in case we need to look at it during initialization
      part_op->initialize_by_image_range(this, pid, projection, parent, 
                                         fid, id, tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_partition_by_image_range call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_preimage(
                                                  IndexPartition projection,
                                                  LogicalRegion handle,
                                                  LogicalRegion parent,
                                                  FieldID fid,
                                                  IndexSpace color_space,
                                                  PartitionKind part_kind,
                                                  Color color,
                                                  MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         handle.get_index_space().get_tree_id(),
                         parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by preimage in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      // Allocate the partition operation
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op(); 
      ApEvent term_event = part_op->get_completion_event();
      // If the source of the preimage is disjoint then the result is disjoint
      // Note this only applies here and not to range
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p = runtime->forest->get_node(projection);
        if (p->is_disjoint(true/*from app*/))
        {
          if (part_kind == LEGION_COMPUTE_KIND)
            part_kind = LEGION_DISJOINT_KIND;
          else if (part_kind == LEGION_COMPUTE_COMPLETE_KIND)
            part_kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            part_kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
                                       handle.get_index_space(), color_space, 
                                       part_color, part_kind, did, term_event);
      // Do this after creating the pending partition so the node exists
      // in case we need to look at it during initialization
      part_op->initialize_by_preimage(this, pid, projection, handle, 
                                      parent, fid, id, tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_partition_by_preimage call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_partition_by_preimage_range(
                                                  IndexPartition projection,
                                                  LogicalRegion handle,
                                                  LogicalRegion parent,
                                                  FieldID fid,
                                                  IndexSpace color_space,
                                                  PartitionKind part_kind,
                                                  Color color,
                                                  MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         handle.get_index_space().get_tree_id(),
                         parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating partition by preimage range in task %s "
                      "(ID %lld)", get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      // Allocate the partition operation
      DependentPartitionOp *part_op = 
        runtime->get_available_dependent_partition_op(); 
      ApEvent term_event = part_op->get_completion_event();
      // Tell the region tree forest about this partition
      RtEvent safe = runtime->forest->create_pending_partition(this, pid, 
                                       handle.get_index_space(), color_space, 
                                       part_color, part_kind, did, term_event);
      // Do this after creating the pending partition so the node exists
      // in case we need to look at it during initialization
      part_op->initialize_by_preimage_range(this, pid, projection, handle,
                                            parent, fid, id, tag);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around create_partition_by_preimage_range call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition InnerContext::create_pending_partition(
                                                IndexSpace parent,
                                                IndexSpace color_space, 
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexPartition pid(runtime->get_unique_index_partition_id(), 
                         parent.get_tree_id(), parent.get_type_tag());
      DistributedID did = runtime->get_available_distributed_id();
#ifdef DEBUG_LEGION
      log_index.debug("Creating pending partition in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      LegionColor part_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      size_t color_space_size = runtime->forest->get_domain_volume(color_space);
      const ApBarrier partition_ready(
                     Realm::Barrier::create_barrier(color_space_size));
      RtEvent safe = runtime->forest->create_pending_partition(this, pid,parent,
        color_space, part_color, part_kind,did,partition_ready,partition_ready);
      // Wait for any notifications to occur before returning
      if (safe.exists())
        safe.wait();
      if (runtime->verify_partitions)
      {
        // We can't block to check this here because the user needs 
        // control back in order to fill in the pieces of the partitions
        // so just launch a meta-task to check it when we can
        VerifyPartitionArgs args(this, pid, verify_kind, __func__);
        runtime->issue_runtime_meta_task(args, LG_LOW_PRIORITY, 
            Runtime::protect_event(partition_ready));
      }
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space_union(IndexPartition parent,
                                                      const void *realm_color,
                                                      size_t color_size,
                                                      TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space union in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag);
      part_op->initialize_index_space_union(this, result, handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space_union(IndexPartition parent,
                                                      const void *realm_color,
                                                      size_t color_size,
                                                      TypeTag type_tag,
                                                      IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space union in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag);
      part_op->initialize_index_space_union(this, result, handle);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space_intersection(
                                                      IndexPartition parent,
                                                      const void *realm_color,
                                                      size_t color_size,
                                                      TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space intersection in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_intersection(this, result, handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space_intersection(
                                                      IndexPartition parent,
                                                      const void *realm_color,
                                                      size_t color_size,
                                                      TypeTag type_tag,
                                                      IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space intersection in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_intersection(this, result, handle);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace InnerContext::create_index_space_difference(
                                                    IndexPartition parent,
                                                    const void *realm_color,
                                                    size_t color_size,
                                                    TypeTag type_tag,
                                                    IndexSpace initial,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space difference in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      PendingPartitionOp *part_op = 
        runtime->get_available_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_difference(this, result, initial,handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    } 

    //--------------------------------------------------------------------------
    void InnerContext::verify_partition(IndexPartition pid, PartitionKind kind,
                                        const char *function_name)
    //--------------------------------------------------------------------------
    {
      IndexPartNode *node = runtime->forest->get_node(pid);
      // Check containment first because our implementation of the algorithms
      // for disjointnss and completeness rely upon it.
      if (node->total_children == node->max_linearized_color)
      {
        for (LegionColor color = 0; color < node->total_children; color++)
        {
          IndexSpaceNode *child_node = node->get_child(color);
          IndexSpaceExpression *diff = 
            runtime->forest->subtract_index_spaces(child_node, node->parent);
          if (!diff->is_empty())
          {
            const DomainPoint bad = 
              node->color_space->delinearize_color_to_point(color);
            switch (bad.get_dim())
            {
              case 1:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0])
              case 2:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1])
              case 3:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2])
              case 4:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3])
              case 5:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4])
              case 6:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5])
              case 7:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6])
              case 8:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7])
              case 9:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7], bad[8])
              default:
                assert(false);
            }
          }
        }
      }
      else
      {
        ColorSpaceIterator *itr =
          node->color_space->create_color_space_iterator();
        while (itr->is_valid())
        {
          const LegionColor color = itr->yield_color();
          IndexSpaceNode *child_node = node->get_child(color);
          IndexSpaceExpression *diff = 
            runtime->forest->subtract_index_spaces(child_node, node->parent);
          if (!diff->is_empty())
          {
            const DomainPoint bad = 
              node->color_space->delinearize_color_to_point(color);
            switch (bad.get_dim())
            {
              case 1:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0])
              case 2:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1])
              case 3:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2])
              case 4:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3])
              case 5:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4])
              case 6:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5])
              case 7:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6])
              case 8:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7])
              case 9:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7], bad[8])
              default:
                assert(false);
            }
          }
        }
        delete itr;
      }
      // Check disjointness
      if ((kind == LEGION_DISJOINT_KIND) || 
          (kind == LEGION_DISJOINT_COMPLETE_KIND) ||
          (kind == LEGION_DISJOINT_INCOMPLETE_KIND))
      {
        if (!node->is_disjoint(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is aliased.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_KIND) ? "DISJOINT_KIND" :
              (kind == LEGION_DISJOINT_COMPLETE_KIND) ? "DISJOINT_COMPLETE_KIND"
              : "DISJOINT_INCOMPLETE_KIND")
      }
      else if ((kind == LEGION_ALIASED_KIND) || 
               (kind == LEGION_ALIASED_COMPLETE_KIND) ||
               (kind == LEGION_ALIASED_INCOMPLETE_KIND))
      {
        if (node->is_disjoint(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is disjoint.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_ALIASED_KIND) ? "ALIASED_KIND" :
              (kind == LEGION_ALIASED_COMPLETE_KIND) ? "ALIASED_COMPLETE_KIND" :
              "ALIASED_INCOMPLETE_KIND")
      }
      // Check completeness
      if ((kind == LEGION_DISJOINT_COMPLETE_KIND) || 
          (kind == LEGION_ALIASED_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_COMPLETE_KIND))
      {
        if (!node->is_complete(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is incomplete.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_COMPLETE_KIND) ? "DISJOINT_COMPLETE_KIND" 
            : (kind == LEGION_ALIASED_COMPLETE_KIND) ? "ALIASED_COMPLETE_KIND" :
              "COMPUTE_COMPLETE_KIND")
      }
      else if ((kind == LEGION_DISJOINT_INCOMPLETE_KIND) || 
               (kind == LEGION_ALIASED_INCOMPLETE_KIND) || 
               (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        if (node->is_complete(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is complete.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_INCOMPLETE_KIND) ? 
                "DISJOINT_INCOMPLETE_KIND" :
              (kind == LEGION_ALIASED_INCOMPLETE_KIND) ? 
              "ALIASED_INCOMPLETE_KIND" : "COMPUTE_INCOMPLETE_KIND")
      } 
    }

    //--------------------------------------------------------------------------
    /*static*/void InnerContext::handle_partition_verification(const void *args)
    //--------------------------------------------------------------------------
    {
      const VerifyPartitionArgs *vargs = (const VerifyPartitionArgs*)args;
      vargs->proxy_this->verify_partition(vargs->pid, vargs->kind, vargs->func);
    }

    //--------------------------------------------------------------------------
    FieldSpace InnerContext::create_field_space(void)
    //--------------------------------------------------------------------------
    {
      return TaskContext::create_field_space();
    }

    //--------------------------------------------------------------------------
    FieldSpace InnerContext::create_field_space(
                                         const std::vector<size_t> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      return TaskContext::create_field_space(sizes, resulting_fields,serdez_id);
    }

    //--------------------------------------------------------------------------
    FieldSpace InnerContext::create_field_space(
                                         const std::vector<Future> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      const FieldSpace space = TaskContext::create_field_space();
      AutoRuntimeCall call(this);
      FieldSpaceNode *node = runtime->forest->get_node(space);
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
          resulting_fields[idx] = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
      }
      for (unsigned idx = 0; idx < sizes.size(); idx++)
        if (sizes[idx].impl == NULL)
          REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
              "Invalid empty future passed to field allocation for field %d "
              "in task %s (UID %lld)", resulting_fields[idx],
              get_task_name(), get_unique_id())
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();  
      const ApEvent ready = creator_op->get_completion_event();
      node->initialize_fields(ready, resulting_fields, serdez_id);
      creator_op->initialize_fields(this, node, resulting_fields, sizes, 
                                    RtEvent::NO_RT_EVENT);
      register_all_field_creations(space, false/*local*/, resulting_fields);
      add_to_dependence_queue(creator_op);
      return space;
    }

    //--------------------------------------------------------------------------
    void InnerContext::destroy_field_space(FieldSpace handle,
                                           const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      log_field.debug("Destroying field space %x in task %s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id());
#endif
      // Check to see if this is one that we should be allowed to destory
      {
        AutoLock priv_lock(privilege_lock);
        std::map<FieldSpace,unsigned>::iterator finder = 
          created_field_spaces.find(handle);
        if (finder != created_field_spaces.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_field_spaces.erase(finder);
          else
            return;
          // Count how many regions are still using this field space
          // that still need to be deleted before we can remove the
          // list of created fields
          std::set<LogicalRegion> latent_regions;
          for (std::map<LogicalRegion,unsigned>::const_iterator it = 
                created_regions.begin(); it != created_regions.end(); it++)
            if (it->first.get_field_space() == handle)
              latent_regions.insert(it->first);
          for (std::map<LogicalRegion,bool>::const_iterator it = 
                local_regions.begin(); it != local_regions.end(); it++)
            if (it->first.get_field_space() == handle)
              latent_regions.insert(it->first);
          if (latent_regions.empty())
          {
            // No remaining regions so we can remove any created fields now
            for (std::set<std::pair<FieldSpace,FieldID> >::iterator it = 
                  created_fields.begin(); it != 
                  created_fields.end(); /*nothing*/)
            {
              if (it->first == handle)
              {
                std::set<std::pair<FieldSpace,FieldID> >::iterator 
                  to_delete = it++;
                created_fields.erase(to_delete);
              }
              else
                it++;
            }
          }
          else
            latent_field_spaces[handle] = latent_regions;
        }
        else
        {
          // If we didn't make this field space, record the deletion
          // and keep going. It will be handled by the context that
          // made the field space
          deleted_field_spaces.push_back(handle);
          return;
        }
      }
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_field_space_deletion(this, handle, unordered);
      add_to_dependence_queue(op, unordered);
    } 

    //--------------------------------------------------------------------------
    FieldID InnerContext::allocate_field(FieldSpace space, 
                                         const Future &field_size,
                                         FieldID fid, bool local,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
            "Local fields do no support allocation with future sizes yet.")
      if (fid == LEGION_AUTO_GENERATE_ID)
        fid = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
      else if (fid >= LEGION_MAX_APPLICATION_FIELD_ID)
        REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
          "Task %s (ID %lld) attempted to allocate a field with "
          "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
          "bound set in legion_config.h", get_task_name(), get_unique_id(), fid)
#endif
      if (field_size.impl == NULL)
        REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
            "Invalid empty future passed to field allocation for field %d "
            "in task %s (UID %lld)", fid, get_task_name(), get_unique_id())
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();  
      const ApEvent ready = creator_op->get_completion_event();
      // Tell the node that we're allocating a field of size zero
      // which will indicate that we'll fill in the size later
      RtEvent precondition;
      FieldSpaceNode *node = runtime->forest->allocate_field(space, ready, fid, 
                                                      serdez_id, precondition);
      creator_op->initialize_field(this, node, fid, field_size, precondition);
      register_field_creation(space, fid, local);
      add_to_dependence_queue(creator_op);
      return fid;
    }

    //--------------------------------------------------------------------------
    void InnerContext::allocate_local_field(FieldSpace space, size_t field_size,
                                          FieldID fid, CustomSerdezID serdez_id,
                                          std::set<RtEvent> &done_events)
    //--------------------------------------------------------------------------
    {
      // See if we've exceeded our local field allocations 
      // for this field space
      AutoLock local_lock(local_field_lock);
      std::vector<LocalFieldInfo> &infos = local_field_infos[space];
      if (infos.size() == runtime->max_local_fields)
        REPORT_LEGION_ERROR(ERROR_EXCEEDED_MAXIMUM_NUMBER_LOCAL_FIELDS,
          "Exceeded maximum number of local fields in "
                      "context of task %s (UID %lld). The maximum "
                      "is currently set to %d, but can be modified "
                      "with the -lg:local flag.", get_task_name(),
                      get_unique_id(), runtime->max_local_fields)
      std::set<unsigned> current_indexes;
      for (std::vector<LocalFieldInfo>::const_iterator it = 
            infos.begin(); it != infos.end(); it++)
        current_indexes.insert(it->index);
      std::vector<FieldID> fields(1, fid);
      std::vector<size_t> sizes(1, field_size);
      std::vector<unsigned> new_indexes;
      if (!runtime->forest->allocate_local_fields(space, fields, sizes, 
                              serdez_id, current_indexes, new_indexes))
        REPORT_LEGION_ERROR(ERROR_UNABLE_ALLOCATE_LOCAL_FIELD,
          "Unable to allocate local field in context of "
                      "task %s (UID %lld) due to local field size "
                      "fragmentation. This situation can be improved "
                      "by increasing the maximum number of permitted "
                      "local fields in a context with the -lg:local "
                      "flag.", get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      assert(new_indexes.size() == 1);
#endif
      // Only need the lock here when modifying since all writes
      // to this data structure are serialized
      infos.push_back(LocalFieldInfo(fid, field_size, serdez_id, 
                                     new_indexes[0], false));
      AutoLock rem_lock(remote_lock,1,false/*exclusive*/);
      // Have to send notifications to any remote nodes
      for (std::map<AddressSpaceID,RemoteContext*>::const_iterator it = 
            remote_instances.begin(); it != remote_instances.end(); it++)
      {
        RtUserEvent done_event = Runtime::create_rt_user_event();
        Serializer rez;
        {
          RezCheck z(rez);
          rez.serialize(it->second);
          rez.serialize<size_t>(1); // field space count
          rez.serialize(space);
          rez.serialize<size_t>(1); // field count
          rez.serialize(infos.back());
          rez.serialize(done_event);
        }
        runtime->send_local_field_update(it->first, rez);
        done_events.insert(done_event);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::allocate_fields(FieldSpace space,
                                       const std::vector<Future> &sizes,
                                       std::vector<FieldID> &resulting_fields,
                                       bool local, CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
            "Local fields do no support allocation with future sizes yet.") 
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
          resulting_fields[idx] = runtime->get_unique_field_id();
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
      }
      for (unsigned idx = 0; idx < sizes.size(); idx++)
        if (sizes[idx].impl == NULL)
          REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
              "Invalid empty future passed to field allocation for field %d "
              "in task %s (UID %lld)", resulting_fields[idx],
              get_task_name(), get_unique_id())
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();  
      const ApEvent ready = creator_op->get_completion_event();
      // Tell the node that we're allocating a field of size zero
      // which will indicate that we'll fill in the size later
      RtEvent precondition;
      FieldSpaceNode *node = runtime->forest->allocate_fields(space, ready, 
                                resulting_fields, serdez_id, precondition);
      creator_op->initialize_fields(this, node, resulting_fields, 
                                    sizes, precondition);
      register_all_field_creations(space, local, resulting_fields);
      add_to_dependence_queue(creator_op);
    }

    //--------------------------------------------------------------------------
    void InnerContext::allocate_local_fields(FieldSpace space,
                                   const std::vector<size_t> &sizes,
                                   const std::vector<FieldID> &resulting_fields,
                                   CustomSerdezID serdez_id,
                                   std::set<RtEvent> &done_events)
    //--------------------------------------------------------------------------
    {
      // See if we've exceeded our local field allocations 
      // for this field space
      AutoLock local_lock(local_field_lock);
      std::vector<LocalFieldInfo> &infos = local_field_infos[space];
      if ((infos.size() + sizes.size()) > runtime->max_local_fields)
        REPORT_LEGION_ERROR(ERROR_EXCEEDED_MAXIMUM_NUMBER_LOCAL_FIELDS,
          "Exceeded maximum number of local fields in "
                      "context of task %s (UID %lld). The maximum "
                      "is currently set to %d, but can be modified "
                      "with the -lg:local flag.", get_task_name(),
                      get_unique_id(), runtime->max_local_fields)
      std::set<unsigned> current_indexes;
      for (std::vector<LocalFieldInfo>::const_iterator it = 
            infos.begin(); it != infos.end(); it++)
        current_indexes.insert(it->index);
      std::vector<unsigned> new_indexes;
      if (!runtime->forest->allocate_local_fields(space, resulting_fields, 
                          sizes, serdez_id, current_indexes, new_indexes))
        REPORT_LEGION_ERROR(ERROR_UNABLE_ALLOCATE_LOCAL_FIELD,
          "Unable to allocate local field in context of "
                      "task %s (UID %lld) due to local field size "
                      "fragmentation. This situation can be improved "
                      "by increasing the maximum number of permitted "
                      "local fields in a context with the -lg:local "
                      "flag.", get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      assert(new_indexes.size() == resulting_fields.size());
#endif
      // Only need the lock here when writing since we know all writes
      // are serialized and we only need to worry about interfering readers
      const unsigned offset = infos.size();
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
        infos.push_back(LocalFieldInfo(resulting_fields[idx], 
                   sizes[idx], serdez_id, new_indexes[idx], false));
      // Have to send notifications to any remote nodes 
      AutoLock rem_lock(remote_lock,1,false/*exclusive*/);
      for (std::map<AddressSpaceID,RemoteContext*>::const_iterator it = 
            remote_instances.begin(); it != remote_instances.end(); it++)
      {
        RtUserEvent done_event = Runtime::create_rt_user_event();
        Serializer rez;
        {
          RezCheck z(rez);
          rez.serialize(it->second);
          rez.serialize<size_t>(1); // field space count
          rez.serialize(space);
          rez.serialize<size_t>(resulting_fields.size()); // field count
          for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
            rez.serialize(infos[offset+idx]);
          rez.serialize(done_event);
        }
        runtime->send_local_field_update(it->first, rez);
        done_events.insert(done_event);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::free_field(FieldAllocatorImpl *allocator, 
                            FieldSpace space, FieldID fid, const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        const std::pair<FieldSpace,FieldID> key(space, fid);
        // This field will actually be removed in analyze_destroy_fields
        std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
          finder = created_fields.find(key);
        if (finder == created_fields.end())
        {
          std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
            local_finder = local_fields.find(key);
          if (local_finder == local_fields.end())
          {
            // If we didn't make this field, record the deletion and
            // then have a later context handle it
            deleted_fields.push_back(key);
            return;
          }
          else
            local_finder->second = true;
        }
        // Don't remove anything from created fields yet, we still might
        // need it as part of the logical dependence analysis for earlier ops
      }
      // Launch off the deletion operation
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_field_deletion(this, space, fid, unordered, allocator,
                                    false/*non owner shard*/);
      add_to_dependence_queue(op, unordered);
    } 

    //--------------------------------------------------------------------------
    void InnerContext::free_fields(FieldAllocatorImpl *allocator, 
                                   FieldSpace space,
                                   const std::set<FieldID> &to_free,
                                   const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      std::set<FieldID> free_now;
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        // These fields will actually be removed in analyze_destroy_fields
        for (std::set<FieldID>::const_iterator it = 
              to_free.begin(); it != to_free.end(); it++)
        {
          const std::pair<FieldSpace,FieldID> key(space, *it);
          std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
            finder = created_fields.find(key);
          if (finder == created_fields.end())
          {
            std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
              local_finder = local_fields.find(key);
            if (local_finder != local_fields.end())
            {
              local_finder->second = true;
              free_now.insert(*it);
            }
            else
              deleted_fields.push_back(key);
          }
          else
          {
            // Don't remove anything from created fields yet, 
            // we still might need need it as part of the logical 
            // dependence analysis for earlier ops
            free_now.insert(*it);
          }
        }
      }
      if (free_now.empty())
        return;
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_field_deletions(this, space, free_now, unordered, 
                                     allocator, false/*non owner shard*/);
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    LogicalRegion InnerContext::create_logical_region(RegionTreeForest *forest,
                                                      IndexSpace index_space,
                                                      FieldSpace field_space,
                                                      const bool task_local,
                                                      const bool output_region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      RegionTreeID tid = runtime->get_unique_region_tree_id();
      LogicalRegion region(tid, index_space, field_space);
#ifdef DEBUG_LEGION
      log_region.debug("Creating logical region in task %s (ID %lld) with "
                       "index space %x and field space %x in new tree %d",
                       get_task_name(), get_unique_id(), 
                       index_space.id, field_space.id, tid);
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_region(index_space.id, field_space.id, tid);

      const DistributedID did = runtime->get_available_distributed_id();
      forest->create_logical_region(region, did);
      // Register the creation of a top-level region with the context
      register_region_creation(region, task_local, output_region);
      return region;
    }

    //--------------------------------------------------------------------------
    void InnerContext::destroy_logical_region(LogicalRegion handle,
                                              const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      log_region.debug("Deleting logical region (%x,%x) in task %s (ID %lld)",
                       handle.index_space.id, handle.field_space.id, 
                       get_task_name(), get_unique_id());
#endif
      // Check to see if this is a top-level logical region, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_region(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy logical region (%x,%x,%x in task %s "
            "(UID %lld) which is not a top-level logical region. Legion only "
            "permits top-level logical regions to be destroyed.", 
            handle.index_space.id, handle.field_space.id, handle.tree_id,
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<LogicalRegion,unsigned>::iterator finder = 
          created_regions.find(handle);
        if (finder == created_regions.end())
        {
          // Check to see if it is a local region
          std::map<LogicalRegion,bool>::iterator local_finder = 
            local_regions.find(handle);
          // Mark that this region is deleted, safe even though this
          // is a read-only lock because we're not changing the structure
          // of the map
          if (local_finder == local_regions.end())
          {
            // Record the deletion for later and propagate it up
            deleted_regions.push_back(handle);
            return;
          }
          else
            local_finder->second = true;
        }
        else
        {
          if (finder->second == 0)
          {
            REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
                "Duplicate deletions were performed for region (%x,%x,%x) "
                "in task tree rooted by %s", handle.index_space.id, 
                handle.field_space.id, handle.tree_id, get_task_name())
            return;
          }
          if (--finder->second > 0)
            return;
          // Don't remove anything from created regions yet, we still might
          // need it as part of the logical dependence analysis for earlier
          // operations, but the reference count is zero so we're protected
        }
      }
      DeletionOp *op = runtime->get_available_deletion_op();
      op->initialize_logical_region_deletion(this, handle, unordered);
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void InnerContext::advise_analysis_subtree(LogicalRegion parent,
                                   const std::set<LogicalRegion> &regions,
                                   const std::set<LogicalPartition> &partitions,
                                   const std::set<FieldID> &fields)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Ignore advisement calls inside of traces
      if ((current_trace != NULL) && current_trace->is_fixed())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement was made inside of a trace.",
            get_task_name(), get_unique_id())
        return;
      }
      if (fields.empty())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement contains no fields.",
            get_task_name(), get_unique_id())
        return;
      }
      if (regions.empty() && partitions.empty())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement contains no regions and partitions.",
            get_task_name(), get_unique_id())
        return;
      }
      AdvisementOp *advisement = runtime->get_available_advisement_op();
      advisement->initialize(this, parent, regions, partitions, fields);
      add_to_dependence_queue(advisement);
    }

    //--------------------------------------------------------------------------
    void InnerContext::get_local_field_set(const FieldSpace handle,
                                           const std::set<unsigned> &indexes,
                                           std::set<FieldID> &to_set) const
    //--------------------------------------------------------------------------
    {
      AutoLock lf_lock(local_field_lock, 1, false/*exclusive*/);
      std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator
        finder = local_field_infos.find(handle);
#ifdef DEBUG_LEGION
      assert(finder != local_field_infos.end());
      unsigned found = 0;
#endif
      for (std::vector<LocalFieldInfo>::const_iterator it = 
            finder->second.begin(); it != finder->second.end(); it++)
      {
        if (indexes.find(it->index) != indexes.end())
        {
#ifdef DEBUG_LEGION
          found++;
#endif
          to_set.insert(it->fid);
        }
      }
#ifdef DEBUG_LEGION
      assert(found == indexes.size());
#endif
    }

    //--------------------------------------------------------------------------
    void InnerContext::get_local_field_set(const FieldSpace handle,
                                           const std::set<unsigned> &indexes,
                                           std::vector<FieldID> &to_set) const
    //--------------------------------------------------------------------------
    {
      AutoLock lf_lock(local_field_lock, 1, false/*exclusive*/);
      std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator
        finder = local_field_infos.find(handle);
#ifdef DEBUG_LEGION
      assert(finder != local_field_infos.end());
      unsigned found = 0;
#endif
      for (std::vector<LocalFieldInfo>::const_iterator it = 
            finder->second.begin(); it != finder->second.end(); it++)
      {
        if (indexes.find(it->index) != indexes.end())
        {
#ifdef DEBUG_LEGION
          found++;
#endif
          to_set.push_back(it->fid);
        }
      }
#ifdef DEBUG_LEGION
      assert(found == indexes.size());
#endif
    }

    //--------------------------------------------------------------------------
    void InnerContext::add_physical_region(const RegionRequirement &req,
          bool mapped, MapperID mid, MappingTagID tag, ApUserEvent &unmap_event,
          bool virtual_mapped, const InstanceSet &physical_instances)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!unmap_event.exists());
#endif
      if (!virtual_mapped)
        unmap_event = Runtime::create_ap_user_event(NULL);
      PhysicalRegionImpl *impl = new PhysicalRegionImpl(req,
          RtEvent::NO_RT_EVENT, ApEvent::NO_AP_EVENT,
          mapped ? unmap_event : ApUserEvent::NO_AP_USER_EVENT, mapped, this,
          mid, tag, false/*leaf region*/, virtual_mapped, runtime);
      physical_regions.push_back(PhysicalRegion(impl));
      if (!virtual_mapped)
        impl->set_references(physical_instances, true/*safe*/); 
    }

    //--------------------------------------------------------------------------
    Future InnerContext::execute_task(const TaskLauncher &launcher,
                                      std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_task_false(launcher);
      IndividualTask *task = runtime->get_available_individual_task();
      Future result = task->initialize_task(this, launcher,
                                            true/*track parent*/,
                                            false/*top level*/,
                                            false/*implicit top level*/,
                                            outputs);
#ifdef DEBUG_LEGION
      log_task.debug("Registering new single task with unique id %lld "
                      "and task %s (ID %lld) with high level runtime in "
                      "addresss space %d",
                      task->get_unique_id(), task->get_task_name(), 
                      task->get_unique_id(), runtime->address_space);
#endif
      execute_task_launch(task, false/*index*/, current_trace, 
                          launcher.silence_warnings, launcher.enable_inlining);
      return result;
    }

    //--------------------------------------------------------------------------
    FutureMap InnerContext::execute_index_space(
                                        const IndexTaskLauncher &launcher,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (launcher.must_parallelism)
      {
        // Turn around and use a must epoch launcher
        MustEpochLauncher epoch_launcher(launcher.map_id, launcher.tag);
        epoch_launcher.add_index_task(launcher);
        FutureMap result = execute_must_epoch(epoch_launcher);
        return result;
      }
      AutoRuntimeCall call(this);
      if (launcher.launch_domain.exists() && 
          (launcher.launch_domain.get_volume() == 0))
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_IGNORING_EMPTY_INDEX_TASK_LAUNCH,
          "Ignoring empty index task launch in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return FutureMap();
      }
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_index_task_false(
            __sync_add_and_fetch(&outstanding_children_count,1), launcher);
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      IndexTask *task = runtime->get_available_index_task();
      FutureMap result = task->initialize_task(this,
                                               launcher,
                                               launch_space,
                                               true /*track*/,
                                               outputs);
#ifdef DEBUG_LEGION
      log_task.debug("Registering new index space task with unique id "
                     "%lld and task %s (ID %lld) with high level runtime in "
                     "address space %d",
                     task->get_unique_id(), task->get_task_name(), 
                     task->get_unique_id(), runtime->address_space);
#endif
      execute_task_launch(task, true/*index*/, current_trace, 
                          launcher.silence_warnings, launcher.enable_inlining);
      return result;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::execute_index_space(const IndexTaskLauncher &launcher,
                                        ReductionOpID redop, bool deterministic,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (launcher.must_parallelism)
      {
        // Turn around and use a must epoch launcher
        MustEpochLauncher epoch_launcher(launcher.map_id, launcher.tag);
        epoch_launcher.add_index_task(launcher);
        FutureMap result = execute_must_epoch(epoch_launcher);
        return reduce_future_map(result, redop, deterministic,
                                 launcher.map_id, launcher.tag);
      }
      AutoRuntimeCall call(this);
      if (launcher.launch_domain.exists() &&
          (launcher.launch_domain.get_volume() == 0))
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_IGNORING_EMPTY_INDEX_TASK_LAUNCH,
          "Ignoring empty index task launch in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return Future();
      }
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_index_task_reduce_false(launcher);
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      IndexTask *task = runtime->get_available_index_task();
      Future result = task->initialize_task(this, launcher, launch_space, 
                                            redop, deterministic,
                                            true /*track*/, outputs);
#ifdef DEBUG_LEGION
      log_task.debug("Registering new index space task with unique id "
                     "%lld and task %s (ID %lld) with high level runtime in "
                     "address space %d",
                     task->get_unique_id(), task->get_task_name(), 
                     task->get_unique_id(), runtime->address_space);
#endif
      execute_task_launch(task, true/*index*/, current_trace, 
                          launcher.silence_warnings, launcher.enable_inlining);
      return result;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::reduce_future_map(const FutureMap &future_map,
                                        ReductionOpID redop, bool deterministic,
                                        MapperID mapper_id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (future_map.impl == NULL)
        return Future();
      AllReduceOp *all_reduce_op = runtime->get_available_all_reduce_op();
      Future result = all_reduce_op->initialize(this, future_map, redop, 
                                                deterministic, mapper_id, tag);
      add_to_dependence_queue(all_reduce_op);
      return result;
    }

    //--------------------------------------------------------------------------
    FutureMap InnerContext::construct_future_map(const Domain &domain,
                                    const std::map<DomainPoint,Future> &futures,
                                    RtUserEvent domain_deletion, bool internal)
    //--------------------------------------------------------------------------
    {
      if (!internal)
      {
        AutoRuntimeCall call(this);
        if (futures.size() != domain.get_volume())
          REPORT_LEGION_ERROR(ERROR_FUTURE_MAP_COUNT_MISMATCH,
            "The number of futures passed into a future map construction (%zd) "
            "does not match the volume of the domain (%zd) for the future map "
            "in task %s (UID %lld)", futures.size(), domain.get_volume(),
            get_task_name(), get_unique_id())
        return construct_future_map(domain, futures, 
                                    domain_deletion, true/*internal*/);
      }
      CreationOp *creation_op = runtime->get_available_creation_op();
      creation_op->initialize_map(this, futures);
      const DistributedID did = runtime->get_available_distributed_id();
      FutureMapImpl *impl = new FutureMapImpl(this, creation_op, 
                            RtEvent::NO_RT_EVENT, domain, runtime, 
                            did, runtime->address_space, domain_deletion);
      add_to_dependence_queue(creation_op);
      impl->set_all_futures(futures);
      return FutureMap(impl);
    }

    //--------------------------------------------------------------------------
    PhysicalRegion InnerContext::map_region(const InlineLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (IS_NO_ACCESS(launcher.requirement))
        return PhysicalRegion();
      MapOp *map_op = runtime->get_available_map_op();
      PhysicalRegion result = map_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Registering a map operation for region "
                    "(%x,%x,%x) in task %s (ID %lld)",
                    launcher.requirement.region.index_space.id, 
                    launcher.requirement.region.field_space.id, 
                    launcher.requirement.region.tree_id, 
                    get_task_name(), get_unique_id());
#endif
      bool parent_conflict = false, inline_conflict = false;  
      const int index = 
        has_conflicting_regions(map_op, parent_conflict, inline_conflict);
      if (parent_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_INLINE_MAPPING_REGION,
          "Attempted an inline mapping of region "
                      "(%x,%x,%x) that conflicts with mapped region " 
                      "(%x,%x,%x) at index %d of parent task %s "
                      "(ID %lld) that would ultimately result in "
                      "deadlock. Instead you receive this error message.",
                      launcher.requirement.region.index_space.id,
                      launcher.requirement.region.field_space.id,
                      launcher.requirement.region.tree_id,
                      regions[index].region.index_space.id,
                      regions[index].region.field_space.id,
                      regions[index].region.tree_id,
                      index, get_task_name(), get_unique_id())
      if (inline_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_INLINE_MAPPING_REGION,
          "Attempted an inline mapping of region (%x,%x,%x) "
                      "that conflicts with previous inline mapping in "
                      "task %s (ID %lld) that would ultimately result in "
                      "deadlock.  Instead you receive this error message.",
                      launcher.requirement.region.index_space.id,
                      launcher.requirement.region.field_space.id,
                      launcher.requirement.region.tree_id,
                      get_task_name(), get_unique_id())
      register_inline_mapped_region(result);
      add_to_dependence_queue(map_op);
      return result;
    }

    //--------------------------------------------------------------------------
    ApEvent InnerContext::remap_region(PhysicalRegion region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Check to see if the region is already mapped,
      // if it is then we are done
      if (region.is_mapped())
        return ApEvent::NO_AP_EVENT;
      MapOp *map_op = runtime->get_available_map_op();
      map_op->initialize(this, region);
      register_inline_mapped_region(region);
      const ApEvent result = map_op->get_program_order_event();
      add_to_dependence_queue(map_op);
      return result;
    }

    //--------------------------------------------------------------------------
    void InnerContext::unmap_region(PhysicalRegion region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!region.is_mapped())
        return;
      region.impl->unmap_region();
      unregister_inline_mapped_region(region);
    }

    //--------------------------------------------------------------------------
    void InnerContext::unmap_all_regions(bool external)
    //--------------------------------------------------------------------------
    {
      if (external)
      {
        AutoRuntimeCall call(this);
        unmap_all_regions(false);
        return;
      }
      for (std::vector<PhysicalRegion>::const_iterator it = 
            physical_regions.begin(); it != physical_regions.end(); it++)
      {
        if (it->is_mapped())
          it->impl->unmap_region();
      }
      // Also unmap any of our inline mapped physical regions
      AutoLock i_lock(inline_lock);
      for (LegionList<PhysicalRegion,TASK_INLINE_REGION_ALLOC>::
            tracked::const_iterator it = inline_regions.begin();
            it != inline_regions.end(); it++)
      {
        if (it->is_mapped())
          it->impl->unmap_region();
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::fill_fields(const FillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (launcher.fields.empty())
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_EMPTY_FILL_FIELDS,
            "Ignoring fill request with no fields in ask %s (UID %lld)",
            get_task_name(), get_unique_id())
        return;
      }
      FillOp *fill_op = runtime->get_available_fill_op();
      fill_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Registering a fill operation in task %s (ID %lld)",
                     get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(fill_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "WARNING: Runtime is unmapping and remapping "
              "physical regions around fill_fields call in task %s (UID %lld).",
              get_task_name(), get_unique_id());
        }
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(fill_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void InnerContext::fill_fields(const IndexFillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (launcher.fields.empty())
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_EMPTY_FILL_FIELDS,
            "Ignoring index fill request with no fields in ask %s (UID %lld)",
            get_task_name(), get_unique_id())
        return;
      }
      if (launcher.launch_domain.exists() && 
          (launcher.launch_domain.get_volume() == 0))
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_IGNORING_EMPTY_INDEX_SPACE_FILL,
          "Ignoring empty index space fill in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return;
      }
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      IndexFillOp *fill_op = runtime->get_available_index_fill_op();
      fill_op->initialize(this, launcher, launch_space); 
#ifdef DEBUG_LEGION
      log_run.debug("Registering an index fill operation in task %s (ID %lld)",
                     get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(fill_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around fill_fields call in task %s (UID %lld).",
              get_task_name(), get_unique_id());
        }
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(fill_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void InnerContext::issue_copy(const CopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      CopyOp *copy_op = runtime->get_available_copy_op();
      copy_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Registering a copy operation in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(copy_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around issue_copy_operation call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(copy_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void InnerContext::issue_copy(const IndexCopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (launcher.launch_domain.exists() &&
          (launcher.launch_domain.get_volume() == 0))
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_IGNORING_EMPTY_INDEX_SPACE_COPY,
          "Ignoring empty index space copy in task %s "
                        "(ID %lld)", get_task_name(), get_unique_id());
        return;
      }
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      IndexCopyOp *copy_op = runtime->get_available_index_copy_op();
      copy_op->initialize(this, launcher, launch_space); 
#ifdef DEBUG_LEGION
      log_run.debug("Registering an index copy operation in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(copy_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around issue_copy_operation call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(copy_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void InnerContext::issue_acquire(const AcquireLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      AcquireOp *acquire_op = runtime->get_available_acquire_op();
      acquire_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Issuing an acquire operation in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this acquire operation.
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(acquire_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around issue_acquire call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the acquire operation
      add_to_dependence_queue(acquire_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void InnerContext::issue_release(const ReleaseLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      ReleaseOp *release_op = runtime->get_available_release_op();
      release_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Issuing a release operation in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // Check to see if we need to do any unmappings and remappings
      // before we can issue the release operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(release_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around issue_release call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the release operation
      add_to_dependence_queue(release_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    PhysicalRegion InnerContext::attach_resource(const AttachLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      AttachOp *attach_op = runtime->get_available_attach_op();
      PhysicalRegion result = attach_op->initialize(this, launcher);
      bool parent_conflict = false, inline_conflict = false;
      int index = has_conflicting_regions(attach_op, 
                                          parent_conflict, inline_conflict);
      if (parent_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_ATTACH_HDF5,
          "Attempted an attach hdf5 file operation on region "
                      "(%x,%x,%x) that conflicts with mapped region " 
                      "(%x,%x,%x) at index %d of parent task %s (ID %lld) "
                      "that would ultimately result in deadlock. Instead you "
                      "receive this error message. Try unmapping the region "
                      "before invoking attach_hdf5 on file %s",
                      launcher.handle.index_space.id, 
                      launcher.handle.field_space.id, 
                      launcher.handle.tree_id, 
                      regions[index].region.index_space.id,
                      regions[index].region.field_space.id,
                      regions[index].region.tree_id, index, 
                      get_task_name(), get_unique_id(), launcher.file_name)
      if (inline_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_ATTACH_HDF5,
          "Attempted an attach hdf5 file operation on region "
                      "(%x,%x,%x) that conflicts with previous inline "
                      "mapping in task %s (ID %lld) "
                      "that would ultimately result in deadlock. Instead you "
                      "receive this error message. Try unmapping the region "
                      "before invoking attach_hdf5 on file %s",
                      launcher.handle.index_space.id, 
                      launcher.handle.field_space.id, 
                      launcher.handle.tree_id, get_task_name(), 
                      get_unique_id(), launcher.file_name)
      // Add this region to the list of inline mapped regions if it is mapped
      if (result.is_mapped())
        register_inline_mapped_region(result);
      add_to_dependence_queue(attach_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::detach_resource(PhysicalRegion region,
                                         const bool flush, const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Unmap the region here so that it is safe for re-use
      if (region.impl->is_mapped())
      {
        region.impl->unmap_region();
        // Remove this region from the list of inline regions if it is mapped
        unregister_inline_mapped_region(region);
      }
      DetachOp *op = runtime->get_available_detach_op();
      Future result = op->initialize_detach(this, region, flush, unordered);
      add_to_dependence_queue(op, unordered);
      return result;
    }

    //--------------------------------------------------------------------------
    void InnerContext::progress_unordered_operations(void)
    //--------------------------------------------------------------------------
    {
      RtEvent precondition;
      Operation *op = NULL;
      {
        AutoLock d_lock(dependence_lock);
        // If we have any unordered ops and we're not in the middle of
        // a trace then add them into the queue
        if (!unordered_ops.empty() && (current_trace == NULL))
          insert_unordered_ops(d_lock, false/*end task*/, true/*progress*/);
        if (dependence_queue.empty() || outstanding_dependence)
          return;
        outstanding_dependence = true;
        precondition = dependence_precondition;
        dependence_precondition = RtEvent::NO_RT_EVENT;
        op = dependence_queue.front();
      }
      DependenceArgs args(op, this);
      const LgPriority priority = LG_THROUGHPUT_WORK_PRIORITY;
      runtime->issue_runtime_meta_task(args, priority, precondition);
    }

    //--------------------------------------------------------------------------
    FutureMap InnerContext::execute_must_epoch(
                                              const MustEpochLauncher &launcher)
    //--------------------------------------------------------------------------
    {
#ifdef SAFE_MUST_EPOCH_LAUNCHES
      // Must epoch launches can sometimes block on external resources which
      // Realm does not know about. In theory this can lead to deadlock, so
      // we provide this mechanism for ordering must epoch launches. By 
      // inserting an execution fence before every must epoch launche we
      // guarantee that it is ordered with respect to every other one. This
      // is heavy-handed for sure, but it is effective and sound and gives
      // us the property that we want until someone comes up with a use
      // case that proves that they need something better.
      // See github issue #659
      issue_execution_fence();
#endif
      AutoRuntimeCall call(this);
      MustEpochOp *epoch_op = runtime->get_available_epoch_op();
#ifdef DEBUG_LEGION
      log_run.debug("Executing a must epoch in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      FutureMap result = epoch_op->initialize(this, launcher);
      // Now find all the parent task regions we need to invalidate
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        epoch_op->find_conflicted_regions(unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "Runtime is unmapping and remapping "
              "physical regions around issue_release call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        }
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Now we can issue the must epoch
      add_to_dependence_queue(epoch_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      return result;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::issue_timing_measurement(const TimingLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Issuing a timing measurement in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      TimingOp *timing_op = runtime->get_available_timing_op();
      Future result = timing_op->initialize(this, launcher);
      add_to_dependence_queue(timing_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::issue_mapping_fence(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      FenceOp *fence_op = runtime->get_available_fence_op();
#ifdef DEBUG_LEGION
      log_run.debug("Issuing a mapping fence in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      Future f = fence_op->initialize(this, FenceOp::MAPPING_FENCE, true);
      add_to_dependence_queue(fence_op);
      return f;
    }

    //--------------------------------------------------------------------------
    Future InnerContext::issue_execution_fence(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      FenceOp *fence_op = runtime->get_available_fence_op();
#ifdef DEBUG_LEGION
      log_run.debug("Issuing an execution fence in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      Future f = fence_op->initialize(this, FenceOp::EXECUTION_FENCE, true);
      add_to_dependence_queue(fence_op);
      return f; 
    }

    //--------------------------------------------------------------------------
    void InnerContext::complete_frame(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      FrameOp *frame_op = runtime->get_available_frame_op();
#ifdef DEBUG_LEGION
      log_run.debug("Issuing a frame in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      frame_op->initialize(this);
      add_to_dependence_queue(frame_op);
    }

    //--------------------------------------------------------------------------
    Predicate InnerContext::create_predicate(const Future &f)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (f.impl == NULL)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_PREDICATE_CREATION,
          "Illegal predicate creation performed on "
                      "empty future inside of task %s (ID %lld).",
                      get_task_name(), get_unique_id())
      FuturePredOp *pred_op = runtime->get_available_future_pred_op();
      // Hold a reference before initialization
      Predicate result(pred_op);
      pred_op->initialize(this, f);
      add_to_dependence_queue(pred_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Predicate InnerContext::predicate_not(const Predicate &p)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      NotPredOp *pred_op = runtime->get_available_not_pred_op();
      // Hold a reference before initialization
      Predicate result(pred_op);
      pred_op->initialize(this, p);
      add_to_dependence_queue(pred_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Predicate InnerContext::create_predicate(const PredicateLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (launcher.predicates.empty())
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_PREDICATE_CREATION,
          "Illegal predicate creation performed on a "
                      "set of empty previous predicates in task %s (ID %lld).",
                      get_task_name(), get_unique_id())
      else if (launcher.predicates.size() == 1)
        return launcher.predicates[0];
      if (launcher.and_op)
      {
        // Check for short circuit cases
        std::vector<Predicate> actual_predicates;
        for (std::vector<Predicate>::const_iterator it = 
              launcher.predicates.begin(); it != 
              launcher.predicates.end(); it++)
        {
          if ((*it) == Predicate::FALSE_PRED)
            return Predicate::FALSE_PRED;
          else if ((*it) == Predicate::TRUE_PRED)
            continue;
          actual_predicates.push_back(*it);
        }
        if (actual_predicates.empty()) // they were all true
          return Predicate::TRUE_PRED;
        else if (actual_predicates.size() == 1)
          return actual_predicates[0];
        AndPredOp *pred_op = runtime->get_available_and_pred_op();
        // Hold a reference before initialization
        Predicate result(pred_op);
        pred_op->initialize(this, actual_predicates);
        add_to_dependence_queue(pred_op);
        return result;
      }
      else
      {
        // Check for short circuit cases
        std::vector<Predicate> actual_predicates;
        for (std::vector<Predicate>::const_iterator it = 
              launcher.predicates.begin(); it != 
              launcher.predicates.end(); it++)
        {
          if ((*it) == Predicate::TRUE_PRED)
            return Predicate::TRUE_PRED;
          else if ((*it) == Predicate::FALSE_PRED)
            continue;
          actual_predicates.push_back(*it);
        }
        if (actual_predicates.empty()) // they were all false
          return Predicate::FALSE_PRED;
        else if (actual_predicates.size() == 1)
          return actual_predicates[0];
        OrPredOp *pred_op = runtime->get_available_or_pred_op();
        // Hold a reference before initialization
        Predicate result(pred_op);
        pred_op->initialize(this, actual_predicates);
        add_to_dependence_queue(pred_op);
        return result;
      }
    }

    //--------------------------------------------------------------------------
    Future InnerContext::get_predicate_future(const Predicate &p)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (p == Predicate::TRUE_PRED)
      {
        Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
        const bool value = true;
        result.impl->set_local(&value, sizeof(value));
        return result;
      }
      else if (p == Predicate::FALSE_PRED)
      {
        Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
        const bool value = false;
        result.impl->set_local(&value, sizeof(value));
        return result;
      }
      else
      {
#ifdef DEBUG_LEGION
        assert(p.impl != NULL);
#endif
        return p.impl->get_future_result(); 
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::perform_window_wait(void)
    //--------------------------------------------------------------------------
    {
      RtEvent wait_event;
      // Take the context lock in exclusive mode
      {
        AutoLock child_lock(child_op_lock);
        // We already hold our lock from the callsite above
        // Outstanding children count has already been incremented for the
        // operation being launched so decrement it in case we wait and then
        // re-increment it when we wake up again
        int diff = -1; // Need this for PGI dumbness
        const int outstanding_count = 
          __sync_fetch_and_add(&outstanding_children_count, diff);
        // We already decided to wait, so we need to wait for any hysteresis
        // to play a role here
        if (outstanding_count >
            int((100 - context_configuration.hysteresis_percentage) *
                context_configuration.max_window_size / 100))
        {
#ifdef DEBUG_LEGION
          assert(!valid_wait_event);
#endif
          window_wait = Runtime::create_rt_user_event();
          valid_wait_event = true;
          wait_event = window_wait;
        }
      }
      begin_task_wait(false/*from runtime*/);
      wait_event.wait();
      end_task_wait();
      // Re-increment the count once we are awake again
      __sync_fetch_and_add(&outstanding_children_count,1);
    }

    //--------------------------------------------------------------------------
    void InnerContext::add_to_prepipeline_queue(Operation *op)
    //--------------------------------------------------------------------------
    {
      bool issue_task = false;
      const GenerationID gen = op->get_generation();
      {
        AutoLock p_lock(prepipeline_lock);
        prepipeline_queue.push_back(std::pair<Operation*,GenerationID>(op,gen));
        // No need to have more outstanding tasks than there are processors
        if (outstanding_prepipeline < runtime->num_utility_procs)
        {
          const size_t needed_in_flight = 
            (prepipeline_queue.size() + 
             context_configuration.meta_task_vector_width - 1) / 
              context_configuration.meta_task_vector_width;
          if (outstanding_prepipeline < needed_in_flight)
          {
            outstanding_prepipeline++;
            issue_task = true;
          }
        }
      }
      if (issue_task)
      {
        add_reference();
        PrepipelineArgs args(op, this);
        runtime->issue_runtime_meta_task(args, LG_THROUGHPUT_WORK_PRIORITY);
      }
    }

    //--------------------------------------------------------------------------
    bool InnerContext::process_prepipeline_stage(void)
    //--------------------------------------------------------------------------
    {
      std::vector<std::pair<Operation*,GenerationID> > to_perform;
      to_perform.reserve(context_configuration.meta_task_vector_width);
      Operation *launch_next_op = NULL;
      {
        AutoLock p_lock(prepipeline_lock);
        for (unsigned idx = 0; idx < 
              context_configuration.meta_task_vector_width; idx++)
        {
          if (prepipeline_queue.empty())
            break;
          to_perform.push_back(prepipeline_queue.front());
          prepipeline_queue.pop_front();
        }
#ifdef DEBUG_LEGION
        assert(outstanding_prepipeline > 0);
        assert(outstanding_prepipeline <= runtime->num_utility_procs);
#endif
        if (!prepipeline_queue.empty())
        {
          const size_t needed_in_flight = 
            (prepipeline_queue.size() + 
             context_configuration.meta_task_vector_width - 1) / 
              context_configuration.meta_task_vector_width;
          if (outstanding_prepipeline <= needed_in_flight)
            launch_next_op = prepipeline_queue.back().first; 
          else
            outstanding_prepipeline--;
        }
        else
          outstanding_prepipeline--;
      }
      // Perform our prepipeline tasks
      for (std::vector<std::pair<Operation*,GenerationID> >::const_iterator it =
            to_perform.begin(); it != to_perform.end(); it++)
        it->first->execute_prepipeline_stage(it->second, false/*need wait*/);
      if (launch_next_op != NULL)
      {
        // This could maybe give a bad op ID for profiling, but it
        // will not impact the correctness of the code
        PrepipelineArgs args(launch_next_op, this);
        runtime->issue_runtime_meta_task(args, LG_THROUGHPUT_WORK_PRIORITY);
        // Reference keeps flowing with the continuation
        return false;
      }
      else
        return true;
    }

    //--------------------------------------------------------------------------
    ApEvent InnerContext::add_to_dependence_queue(Operation *op, bool unordered, 
                                                  bool outermost)
    //--------------------------------------------------------------------------
    {
      // Launch the task to perform the prepipeline stage for the operation
      if (op->has_prepipeline_stage())
        add_to_prepipeline_queue(op);
      LgPriority priority = LG_THROUGHPUT_WORK_PRIORITY; 
      // If this is tracking, add it to our data structure first
      if (op->is_tracking_parent() || unordered)
      {
        AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
        assert(executing_children.find(op) == executing_children.end());
        assert(executed_children.find(op) == executed_children.end());
        assert(complete_children.find(op) == complete_children.end());
        outstanding_children[op->get_ctx_index()] = op;
#endif       
        executing_children[op] = op->get_generation();
        // Bump our priority if the context is not active as it means
        // that the runtime is currently not ahead of execution
        if (!currently_active_context)
          priority = LG_THROUGHPUT_DEFERRED_PRIORITY;
      }
      
      bool issue_task = false;
      RtEvent precondition;
      ApEvent term_event;
      // We disable program order execution when we are replaying a
      // fixed trace since it might not be sound to block
      if (runtime->program_order_execution && !unordered && outermost &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
        term_event = op->get_program_order_event();
      {
        AutoLock d_lock(dependence_lock);
        if (unordered)
        {
          // If this is unordered, stick it on the list of 
          // unordered ops to be added later and then we're done
          unordered_ops.push_back(op);
          return term_event;
        }
        // Insert any unordered operations into the stream
        insert_unordered_ops(d_lock, false/*end task*/, false/*progress*/);
        dependence_queue.push_back(op);
        if (!outstanding_dependence)
        {
          issue_task = true;
          outstanding_dependence = true;
          precondition = dependence_precondition;
          dependence_precondition = RtEvent::NO_RT_EVENT;
        }
      }
      if (issue_task)
      {
        DependenceArgs args(op, this);
        runtime->issue_runtime_meta_task(args, priority, precondition); 
      }
      if (term_event.exists()) 
      {
        begin_task_wait(true/*from runtime*/);
        bool poisoned = false;
        term_event.wait_faultaware(poisoned);
        if (poisoned)
          raise_poison_exception();
        end_task_wait();
      }
      return term_event;
    }

    //--------------------------------------------------------------------------
    void InnerContext::process_dependence_stage(void)
    //--------------------------------------------------------------------------
    {
      std::vector<Operation*> to_perform;
      to_perform.reserve(context_configuration.meta_task_vector_width);
      Operation *launch_next_op = NULL;
      {
        AutoLock d_lock(dependence_lock);
        for (unsigned idx = 0; idx < 
              context_configuration.meta_task_vector_width; idx++)
        {
          if (dependence_queue.empty())
            break;
          to_perform.push_back(dependence_queue.front());
          dependence_queue.pop_front();
        }
#ifdef DEBUG_LEGION
        assert(outstanding_dependence);
#endif
        if (dependence_queue.empty())
        {
          outstanding_dependence = false;
          // Guard ourselves against tasks running after us
          dependence_precondition = 
            RtEvent(Processor::get_current_finish_event());
        }
        else
          launch_next_op = dependence_queue.front();
      }
      // Perform our operations
      for (std::vector<Operation*>::const_iterator it = 
            to_perform.begin(); it != to_perform.end(); it++)
        (*it)->execute_dependence_analysis();
      // Then launch the next task if needed
      if (launch_next_op != NULL)
      {
        DependenceArgs args(launch_next_op, this);
        // Sample currently_active without the lock to try to get our priority
        runtime->issue_runtime_meta_task(args, !currently_active_context ? 
              LG_THROUGHPUT_DEFERRED_PRIORITY : LG_THROUGHPUT_WORK_PRIORITY);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::add_to_post_task_queue(TaskContext *ctx, RtEvent wait_on,
                                              FutureInstance *instance,
                                              FutureFunctor *callback_functor,
                                              bool own_callback_functor)
    //--------------------------------------------------------------------------
    {
      bool issue_task = false;
      RtEvent precondition;
      const size_t task_index = ctx->get_owner_task()->get_context_index();
      {
        AutoLock p_lock(post_task_lock);
        // Issue a task if there isn't one running right now
        if (post_task_queue.empty())
        {
          issue_task = true;
          // Add a reference to the context the first time we defer this
          add_reference();
        }
        post_task_queue.push_back(PostTaskArgs(ctx, task_index, wait_on,
                      instance, callback_functor, own_callback_functor));
        if (post_task_comp_queue.exists())
        {
          // If we've already got a completion queue then use it
          post_task_comp_queue.add_event(wait_on);
        }
        else if (post_task_queue.size() >= 
                  context_configuration.meta_task_vector_width)
        {
          // Make a completion queue to use
          post_task_comp_queue = CompletionQueue::create_completion_queue(0);
          // Fill in the completion queue with events
          for (std::list<PostTaskArgs>::const_iterator it = 
                post_task_queue.begin(); it != post_task_queue.end(); it++)
            post_task_comp_queue.add_event(it->wait_on);
        }
        if (issue_task)
        {
          if (!post_task_comp_queue.exists())
          {
            // Find the one with the minimum index
            size_t min_index = 0;
            for (std::list<PostTaskArgs>::const_iterator it = 
                  post_task_queue.begin(); it != post_task_queue.end(); it++)
            {
              if (!precondition.exists() || (it->index < min_index))
              {
                precondition = it->wait_on;
                min_index = it->index;
              }
            }
          }
          else
            precondition = RtEvent(post_task_comp_queue.get_nonempty_event());
        }
      }
      if (issue_task)
      {
        // Other things could be added to the queue by the time we're here
        PostEndArgs args(ctx->owner_task, this);
        runtime->issue_runtime_meta_task(args, 
            LG_THROUGHPUT_DEFERRED_PRIORITY, precondition);
      }
    }

    //--------------------------------------------------------------------------
    bool InnerContext::process_post_end_tasks(void)
    //--------------------------------------------------------------------------
    {
      RtEvent precondition;
      TaskContext *next_ctx = NULL;
      std::vector<PostTaskArgs> to_perform;
      to_perform.reserve(context_configuration.meta_task_vector_width);
      {
        std::vector<RtEvent> ready_events(
                          context_configuration.meta_task_vector_width);
        AutoLock p_lock(post_task_lock);
        // Ask the completion queue for the ready events
        size_t num_ready = 0;
        if (!post_task_comp_queue.exists())
        {
          // No completion queue so go through and do this manually
          for (std::list<PostTaskArgs>::const_iterator it =
                post_task_queue.begin(); it != post_task_queue.end(); it++)
          {
            if (it->wait_on.has_triggered())
            {
              ready_events[num_ready++] = it->wait_on;
              if (num_ready == ready_events.size())
                break;
            }
          }
        }
        else // We can just use the comp queue to get the ready events
          num_ready = post_task_comp_queue.pop_events(
            &ready_events.front(), ready_events.size());
#ifdef DEBUG_LEGION
        assert(num_ready > 0);
#endif
        // Find all the entries for all the ready events
        for (std::list<PostTaskArgs>::iterator it = post_task_queue.begin();
              it != post_task_queue.end(); /*nothing*/)
        {
          bool found = false;
          for (unsigned idx = 0; idx < num_ready; idx++)
          {
            if (it->wait_on == ready_events[idx])
            {
              found = true;
              break;
            }
          }
          if (found)
          {
            to_perform.push_back(*it);
            it = post_task_queue.erase(it);
            // Check to see if we're done early
            if (to_perform.size() == num_ready)
              break;
          }
          else
            it++;
        }
        if (!post_task_queue.empty())
        {
          if (!post_task_comp_queue.exists())
          {
            // Find the one with the minimum index
            size_t min_index = 0;
            for (std::list<PostTaskArgs>::const_iterator it =
                  post_task_queue.begin(); it != post_task_queue.end(); it++)
            {
              if (!precondition.exists() || (it->index < min_index))
              {
                precondition = it->wait_on;
                min_index = it->index;
                next_ctx = it->context;
              }
            }
          }
          else
          {
            precondition = RtEvent(post_task_comp_queue.get_nonempty_event());
            next_ctx = post_task_queue.front().context;
          }
#ifdef DEBUG_LEGION
          assert(next_ctx != NULL);
#endif
        }
      }
      // Launch this first to get it in flight so it can run when ready
      if (next_ctx != NULL)
      {
        PostEndArgs args(next_ctx->owner_task, this);
        runtime->issue_runtime_meta_task(args,
            LG_THROUGHPUT_DEFERRED_PRIORITY, precondition);
      }
      // Now perform our operations
      if (!to_perform.empty())
      {
        // Sort these into order by their index before we perform them
        // so we do them in order or we could risk a hang
        std::sort(to_perform.begin(), to_perform.end());
        for (std::vector<PostTaskArgs>::const_iterator it =
              to_perform.begin(); it != to_perform.end(); it++)
          it->context->post_end_task(it->instance, it->functor,it->own_functor);
      }
      // If we didn't launch a next op, then we can remove the reference
      return (next_ctx == NULL);
    }

    //--------------------------------------------------------------------------
    ApBarrier InnerContext::create_phase_barrier(unsigned arrivals,
                                                 ReductionOpID redop,
                                                 const void *init_value,
                                                 size_t init_size)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Creating application barrier in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      return ApBarrier(Realm::Barrier::create_barrier(arrivals, redop,
                                                      init_value, init_size));
    }

    //--------------------------------------------------------------------------
    void InnerContext::destroy_phase_barrier(ApBarrier bar)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Destroying phase barrier in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      destroy_user_barrier(bar);
    }

    //--------------------------------------------------------------------------
    PhaseBarrier InnerContext::advance_phase_barrier(PhaseBarrier bar)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Advancing phase barrier in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      PhaseBarrier result = bar;
      Runtime::advance_barrier(result);
#ifdef LEGION_SPY
      LegionSpy::log_event_dependence(bar.phase_barrier, result.phase_barrier);
#endif
      return result;
    }

    //--------------------------------------------------------------------------
    void InnerContext::arrive_dynamic_collective(DynamicCollective dc,
                                                 const void *buffer,
                                                 size_t size, unsigned count)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Arrive dynamic collective in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      Runtime::phase_barrier_arrive(dc, count, ApEvent::NO_AP_EVENT, 
                                    buffer, size);
    }

    //--------------------------------------------------------------------------
    void InnerContext::defer_dynamic_collective_arrival(DynamicCollective dc,
                                                        const Future &f,
                                                        unsigned count)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Defer dynamic collective arrival in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      // Record this future as a contribution to the collective
      // for future dependence analysis
      record_dynamic_collective_contribution(dc, f);
      f.impl->contribute_to_collective(dc, count);
    }

    //--------------------------------------------------------------------------
    Future InnerContext::get_dynamic_collective_result(DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Get dynamic collective result in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      DynamicCollectiveOp *collective = 
        runtime->get_available_dynamic_collective_op();
      Future result = collective->initialize(this, dc);
      add_to_dependence_queue(collective);
      return result;
    }

    //--------------------------------------------------------------------------
    DynamicCollective InnerContext::advance_dynamic_collective( 
                                                           DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Advancing dynamic collective in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      DynamicCollective result = dc;
      Runtime::advance_barrier(result);
#ifdef LEGION_SPY
      LegionSpy::log_event_dependence(dc.phase_barrier, result.phase_barrier);
#endif
      return result;
    }

    //--------------------------------------------------------------------------
    size_t InnerContext::register_new_child_operation(Operation *op,
                      const std::vector<StaticDependence> *dependences)
    //--------------------------------------------------------------------------
    {
      // If we are performing a trace mark that the child has a trace
      if (current_trace != NULL)
        op->set_trace(current_trace, dependences);
      size_t result = total_children_count++;
      const size_t outstanding_count =
        __sync_add_and_fetch(&outstanding_children_count,1);
      // Only need to check if we are not tracing by frames
      if ((context_configuration.min_frames_to_schedule == 0) &&
          (context_configuration.max_window_size > 0) &&
            (outstanding_count > context_configuration.max_window_size))
        perform_window_wait();
      if (runtime->legion_spy_enabled)
        LegionSpy::log_child_operation_index(get_context_uid(), result,
                                             op->get_unique_op_id());
      return result;
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_new_internal_operation(InternalOp *op)
    //--------------------------------------------------------------------------
    {
      // Nothing to do
    }

    //--------------------------------------------------------------------------
    void InnerContext::insert_unordered_ops(AutoLock &d_lock, 
                                       const bool end_task, const bool progress)
    //--------------------------------------------------------------------------
    {
      // If there are no unordered ops then we're done
      if (unordered_ops.empty())
        return;
      // If we're still in the middle of a trace then don't do any insertions
      if (current_trace != NULL)
        return;
      for (std::list<Operation*>::const_iterator it = 
            unordered_ops.begin(); it != unordered_ops.end(); it++)
      {
        (*it)->set_tracking_parent(total_children_count++);
        dependence_queue.push_back(*it);
      }
      __sync_fetch_and_add(&outstanding_children_count, unordered_ops.size());
      unordered_ops.clear();
    }

    //--------------------------------------------------------------------------
    size_t InnerContext::register_new_close_operation(CloseOp *op)
    //--------------------------------------------------------------------------
    {
      // For now we just bump our counter
      size_t result = total_close_count++;
      if (runtime->legion_spy_enabled)
        LegionSpy::log_close_operation_index(get_context_uid(), result, 
                                             op->get_unique_op_id());
      return result;
    }

    //--------------------------------------------------------------------------
    size_t InnerContext::register_new_summary_operation(TraceSummaryOp *op)
    //--------------------------------------------------------------------------
    {
      // For now we just bump our counter
      size_t result = total_summary_count++;
      const size_t outstanding_count = 
        __sync_add_and_fetch(&outstanding_children_count,1);
      // Only need to check if we are not tracing by frames
      if ((context_configuration.min_frames_to_schedule == 0) && 
          (context_configuration.max_window_size > 0) && 
            (outstanding_count > context_configuration.max_window_size))
        perform_window_wait();
      if (runtime->legion_spy_enabled)
        LegionSpy::log_child_operation_index(get_context_uid(), result, 
                                             op->get_unique_op_id()); 
      return result;
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_executing_child(Operation *op)
    //--------------------------------------------------------------------------
    {
      AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
      assert(executing_children.find(op) == executing_children.end());
#endif
      executing_children[op] = op->get_generation();
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_child_executed(Operation *op)
    //--------------------------------------------------------------------------
    {
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
        std::map<Operation*,GenerationID>::iterator 
          finder = executing_children.find(op);
#ifdef DEBUG_LEGION
        assert(finder != executing_children.end());
        assert(executed_children.find(op) == executed_children.end());
        assert(complete_children.find(op) == complete_children.end());
#endif
        // Now put it in the list of executing operations
        // Note this doesn't change the number of active children
        // so there's no need to trigger any window waits
        executed_children.insert(*finder);
        executing_children.erase(finder);
        // Add some hysteresis here so that we have some runway for when
        // the paused task resumes it can run for a little while.
        int diff = -1; // Need this for PGI dumbness
        int outstanding_count = 
          __sync_add_and_fetch(&outstanding_children_count, diff);
#ifdef DEBUG_LEGION
        assert(outstanding_count >= 0);
#endif
        if (valid_wait_event && (context_configuration.max_window_size > 0) &&
            (outstanding_count <=
             int((100 - context_configuration.hysteresis_percentage) * 
                 context_configuration.max_window_size / 100)))
        {
          to_trigger = window_wait;
          valid_wait_event = false;
        }
      }
      if (to_trigger.exists())
        Runtime::trigger_event(to_trigger);
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_child_complete(Operation *op)
    //--------------------------------------------------------------------------
    {
      bool needs_trigger = false;
      std::set<ApEvent> child_completion_events;
      {
        AutoLock child_lock(child_op_lock);
        std::map<Operation*,GenerationID>::iterator finder = 
          executed_children.find(op);
#ifdef DEBUG_LEGION
        assert(finder != executed_children.end());
        assert(complete_children.find(op) == complete_children.end());
        assert(executing_children.find(op) == executing_children.end());
#endif
        // Put it on the list of complete children to complete
        complete_children.insert(*finder);
        executed_children.erase(finder);
        // See if we need to trigger the all children complete call
        if (task_executed && (owner_task != NULL) && executing_children.empty()
            && executed_children.empty() && !children_complete_invoked)
        {
          needs_trigger = true;
          children_complete_invoked = true;
          for (LegionMap<Operation*,GenerationID,
                COMPLETE_CHILD_ALLOC>::tracked::const_iterator it =
                complete_children.begin(); it != complete_children.end(); it++)
            child_completion_events.insert(it->first->get_completion_event());
        }
      }
      if (needs_trigger)
      {
        if (!child_completion_events.empty())
          owner_task->trigger_children_complete(
            Runtime::merge_events(NULL, child_completion_events));
        else
          owner_task->trigger_children_complete(ApEvent::NO_AP_EVENT);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::register_child_commit(Operation *op)
    //--------------------------------------------------------------------------
    {
      bool needs_trigger = false;
      {
        AutoLock child_lock(child_op_lock);
        std::map<Operation*,GenerationID>::iterator finder = 
          complete_children.find(op);
#ifdef DEBUG_LEGION
        assert(finder != complete_children.end());
        assert(executing_children.find(op) == executing_children.end());
        assert(executed_children.find(op) == executed_children.end());
        outstanding_children.erase(op->get_ctx_index());
#endif
        complete_children.erase(finder);
        // See if we need to trigger the all children commited call
        if (task_executed && executing_children.empty() && 
            executed_children.empty() && complete_children.empty() &&
            !children_commit_invoked)
        {
          needs_trigger = true;
          children_commit_invoked = true;
        }
      }
      if (needs_trigger && (owner_task != NULL))
        owner_task->trigger_children_committed();
    }

    //--------------------------------------------------------------------------
    int InnerContext::has_conflicting_regions(MapOp *op, bool &parent_conflict,
                                              bool &inline_conflict)
    //--------------------------------------------------------------------------
    {
      const RegionRequirement &req = op->get_requirement(); 
      return has_conflicting_internal(req, parent_conflict, inline_conflict);
    }

    //--------------------------------------------------------------------------
    int InnerContext::has_conflicting_regions(AttachOp *attach,
                                              bool &parent_conflict,
                                              bool &inline_conflict)
    //--------------------------------------------------------------------------
    {
      const RegionRequirement &req = attach->get_requirement();
      return has_conflicting_internal(req, parent_conflict, inline_conflict);
    }

    //--------------------------------------------------------------------------
    int InnerContext::has_conflicting_internal(const RegionRequirement &req,
                                               bool &parent_conflict,
                                               bool &inline_conflict)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, HAS_CONFLICTING_INTERNAL_CALL);
      parent_conflict = false;
      inline_conflict = false;
      // No need to hold our lock here because we are the only ones who
      // could possibly be doing any mutating of the physical_regions data 
      // structure but we are here so we aren't mutating
      for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
      {
        // skip any regions which are not mapped
        if (!physical_regions[our_idx].is_mapped())
          continue;
        const RegionRequirement &our_req = 
          physical_regions[our_idx].impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
        {
          parent_conflict = true;
          return our_idx;
        }
      }
      // Need lock here because of unordered detach operations
      AutoLock i_lock(inline_lock,1,false/*exclusive*/);
      for (std::list<PhysicalRegion>::const_iterator it = 
            inline_regions.begin(); it != inline_regions.end(); it++)
      {
        if (!it->is_mapped())
          continue;
        const RegionRequirement &our_req = it->impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
        {
          inline_conflict = true;
          // No index for inline conflicts
          return -1;
        }
      }
      return -1;
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(TaskOp *task,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      // No need to hold our lock here because we are the only ones who
      // could possibly be doing any mutating of the physical_regions data 
      // structure but we are here so we aren't mutating
      for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
      {
        // Skip any regions which are not mapped
        if (!physical_regions[our_idx].is_mapped())
          continue;
        const RegionRequirement &our_req = 
          physical_regions[our_idx].impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        // Check to see if any region requirements from the child have
        // a dependence on our region at location our_idx
        for (unsigned idx = 0; idx < task->regions.size(); idx++)
        {
          const RegionRequirement &req = task->regions[idx];  
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
          {
            conflicting.push_back(physical_regions[our_idx]);
            // Once we find a conflict, we don't need to check
            // against it anymore, so go onto our next region
            break;
          }
        }
      }
      // Need lock here because of unordered detach operations
      AutoLock i_lock(inline_lock,1,false/*exclusive*/);
      for (std::list<PhysicalRegion>::const_iterator it = 
            inline_regions.begin(); it != inline_regions.end(); it++)
      {
        if (!it->is_mapped())
          continue;
        const RegionRequirement &our_req = it->impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        // Check to see if any region requirements from the child have
        // a dependence on our region at location our_idx
        for (unsigned idx = 0; idx < task->regions.size(); idx++)
        {
          const RegionRequirement &req = task->regions[idx];  
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
          {
            conflicting.push_back(*it);
            // Once we find a conflict, we don't need to check
            // against it anymore, so go onto our next region
            break;
          }
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(CopyOp *copy,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      // No need to hold our lock here because we are the only ones who
      // could possibly be doing any mutating of the physical_regions data 
      // structure but we are here so we aren't mutating
      for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
      {
        // skip any regions which are not mapped
        if (!physical_regions[our_idx].is_mapped())
          continue;
        const RegionRequirement &our_req = 
          physical_regions[our_idx].impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        bool has_conflict = false;
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->src_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->src_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->dst_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->dst_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->src_indirect_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->src_indirect_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->dst_indirect_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->dst_indirect_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        if (has_conflict)
          conflicting.push_back(physical_regions[our_idx]);
      }
      // Need lock here because of unordered detach operations
      AutoLock i_lock(inline_lock,1,false/*exclusive*/);
      for (std::list<PhysicalRegion>::const_iterator it = 
            inline_regions.begin(); it != inline_regions.end(); it++)
      {
        if (!it->is_mapped())
          continue;
        const RegionRequirement &our_req = it->impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        bool has_conflict = false;
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->src_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->src_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->dst_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->dst_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->src_indirect_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->src_indirect_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        for (unsigned idx = 0; !has_conflict &&
              (idx < copy->dst_indirect_requirements.size()); idx++)
        {
          const RegionRequirement &req = copy->dst_indirect_requirements[idx];
          if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
            has_conflict = true;
        }
        if (has_conflict)
          conflicting.push_back(*it);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(AcquireOp *acquire,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      const RegionRequirement &req = acquire->get_requirement();
      find_conflicting_internal(req, conflicting); 
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(ReleaseOp *release,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      const RegionRequirement &req = release->get_requirement();
      find_conflicting_internal(req, conflicting);      
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(DependentPartitionOp *partition,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      const RegionRequirement &req = partition->get_requirement();
      find_conflicting_internal(req, conflicting);
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_internal(const RegionRequirement &req,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      // No need to hold our lock here because we are the only ones who
      // could possibly be doing any mutating of the physical_regions data 
      // structure but we are here so we aren't mutating
      for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
      {
        // skip any regions which are not mapped
        if (!physical_regions[our_idx].is_mapped())
          continue;
        const RegionRequirement &our_req = 
          physical_regions[our_idx].impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
          conflicting.push_back(physical_regions[our_idx]);
      }
      // Need lock here because of unordered detach operations
      AutoLock i_lock(inline_lock,1,false/*exclusive*/);
      for (std::list<PhysicalRegion>::const_iterator it = 
            inline_regions.begin(); it != inline_regions.end(); it++)
      {
        if (!it->is_mapped())
          continue;
        const RegionRequirement &our_req = it->impl->get_requirement();
#ifdef DEBUG_LEGION
        // This better be true for a single task
        assert(our_req.handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        RegionTreeID our_tid = our_req.region.get_tree_id();
        IndexSpace our_space = our_req.region.get_index_space();
        RegionUsage our_usage(our_req);
        if (check_region_dependence(our_tid,our_space,our_req,our_usage,req))
          conflicting.push_back(*it);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_conflicting_regions(FillOp *fill,
                                       std::vector<PhysicalRegion> &conflicting)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, FIND_CONFLICTING_CALL);
      const RegionRequirement &req = fill->get_requirement();
      find_conflicting_internal(req, conflicting);
    } 

    //--------------------------------------------------------------------------
    void InnerContext::register_inline_mapped_region(PhysicalRegion &region)
    //--------------------------------------------------------------------------
    {
      // Because of 'remap_region', this method can be called
      // both for inline regions as well as regions which were
      // initally mapped for the task.  Do a quick check to see
      // if it was an original region.  If it was then we're done.
      for (unsigned idx = 0; idx < physical_regions.size(); idx++)
      {
        if (physical_regions[idx].impl == region.impl)
          return;
      }
      // Need lock because of unordered detach operations
      AutoLock i_lock(inline_lock);
      inline_regions.push_back(region);
    }

    //--------------------------------------------------------------------------
    void InnerContext::unregister_inline_mapped_region(PhysicalRegion &region)
    //--------------------------------------------------------------------------
    {
      // Need lock because of unordered detach operations
      AutoLock i_lock(inline_lock);
      for (std::list<PhysicalRegion>::iterator it = 
            inline_regions.begin(); it != inline_regions.end(); it++)
      {
        if (it->impl == region.impl)
        {
          if (runtime->runtime_warnings && !has_inline_accessor)
            has_inline_accessor = it->impl->created_accessor();
          inline_regions.erase(it);
          return;
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::print_children(void)
    //--------------------------------------------------------------------------
    {
      // Don't both taking the lock since this is for debugging
      // and isn't actually called anywhere
      for (std::map<Operation*,GenerationID>::const_iterator it =
            executing_children.begin(); it != executing_children.end(); it++)
      {
        Operation *op = it->first;
        printf("Executing Child %p\n",op);
      }
      for (std::map<Operation*,GenerationID>::const_iterator it =
            executed_children.begin(); it != executed_children.end(); it++)
      {
        Operation *op = it->first;
        printf("Executed Child %p\n",op);
      }
      for (std::map<Operation*,GenerationID>::const_iterator it =
            complete_children.begin(); it != complete_children.end(); it++)
      {
        Operation *op = it->first;
        printf("Complete Child %p\n",op);
      }
    }

    //--------------------------------------------------------------------------
    ApEvent InnerContext::register_implicit_dependences(Operation *op)
    //--------------------------------------------------------------------------
    {
      // If there are any outstanding unmapped dependent partition operations
      // outstanding then we might have an implicit dependence on its execution
      // so we always record a dependence on it
      if (last_implicit != NULL)
      {
#ifdef LEGION_SPY
        // Can't prune when doing legion spy
        op->register_dependence(last_implicit, last_implicit_gen);
#else
        if (op->register_dependence(last_implicit, last_implicit_gen))
          last_implicit = NULL;
#endif
      }
      if (current_mapping_fence != NULL)
      {
#ifdef LEGION_SPY
        // Can't prune when doing legion spy
        op->register_dependence(current_mapping_fence, mapping_fence_gen);
        unsigned num_regions = op->get_region_count();
        if (num_regions > 0)
        {
          for (unsigned idx = 0; idx < num_regions; idx++)
          {
            LegionSpy::log_mapping_dependence(
                get_unique_id(), current_fence_uid, 0,
                op->get_unique_op_id(), idx, TRUE_DEPENDENCE);
          }
        }
        else
          LegionSpy::log_mapping_dependence(
              get_unique_id(), current_fence_uid, 0,
              op->get_unique_op_id(), 0, TRUE_DEPENDENCE);
#else
        // If we can prune it then go ahead and do so
        // No need to remove the mapping reference because 
        // the fence has already been committed
        if (op->register_dependence(current_mapping_fence, mapping_fence_gen))
          current_mapping_fence = NULL;
#endif
      }
#ifdef LEGION_SPY
      previous_completion_events.insert(op->get_program_order_event());
      // Periodically merge these to keep this data structure from exploding
      // when we have a long-running task, although don't do this for fence
      // operations in case we have to prune ourselves out of the set
      if (previous_completion_events.size() >= LEGION_DEFAULT_MAX_TASK_WINDOW)
      {
        const Operation::OpKind op_kind = op->get_operation_kind(); 
        if ((op_kind != Operation::FENCE_OP_KIND) &&
            (op_kind != Operation::FRAME_OP_KIND) &&
            (op_kind != Operation::DELETION_OP_KIND) &&
            (op_kind != Operation::TRACE_BEGIN_OP_KIND) && 
            (op_kind != Operation::TRACE_COMPLETE_OP_KIND) &&
            (op_kind != Operation::TRACE_CAPTURE_OP_KIND) &&
            (op_kind != Operation::TRACE_REPLAY_OP_KIND) &&
            (op_kind != Operation::TRACE_SUMMARY_OP_KIND))
        {
          const ApEvent merge = 
            Runtime::merge_events(NULL, previous_completion_events);
          previous_completion_events.clear();
          previous_completion_events.insert(merge);
        }
      }
      // Have to record this operation in case there is a fence later
      ops_since_last_fence.push_back(op->get_unique_op_id());
      return current_execution_fence_event;
#else
      if (current_execution_fence_event.exists())
      {
        // We can't have internal operations pruning out fences
        // because we can't test if they are memoizing or not
        // Their 'get_memoizable' method will always return NULL
        bool poisoned = false;
        if (current_execution_fence_event.has_triggered_faultaware(poisoned))
        {
          if (poisoned)
            raise_poison_exception();
          if (!op->is_internal_op())
          {
            // We can only do this optimization safely if we're not 
            // recording a physical trace, otherwise the physical
            // trace needs to see this dependence
            Memoizable *memo = op->get_memoizable();
            if ((memo == NULL) || !memo->is_recording())
              current_execution_fence_event = ApEvent::NO_AP_EVENT;
          }
        }
        return current_execution_fence_event;
      }
      return ApEvent::NO_AP_EVENT;
#endif
    }

    //--------------------------------------------------------------------------
    void InnerContext::perform_fence_analysis(Operation *op, 
               std::set<ApEvent> &previous_events, bool mapping, bool execution)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      {
        const Operation::OpKind op_kind = op->get_operation_kind();
        // It's alright if you hit this assertion for a new operation kind
        // Just add the new operation kind here and then update the check
        // in register_implicit_dependences that looks for all these kinds too
        // so that we do not run into trouble when running with Legion Spy.
        assert((op_kind == Operation::FENCE_OP_KIND) || 
               (op_kind == Operation::FRAME_OP_KIND) || 
               (op_kind == Operation::DELETION_OP_KIND) ||
               (op_kind == Operation::TRACE_BEGIN_OP_KIND) ||
               (op_kind == Operation::TRACE_COMPLETE_OP_KIND) ||
               (op_kind == Operation::TRACE_CAPTURE_OP_KIND) ||
               (op_kind == Operation::TRACE_REPLAY_OP_KIND) ||
               (op_kind == Operation::TRACE_SUMMARY_OP_KIND));
      }
#endif
      std::map<Operation*,GenerationID> previous_operations;
      // Take the lock and iterate through our current pending
      // operations and find all the ones with a context index
      // that is less than the index for the fence operation
      const size_t next_fence_index = op->get_ctx_index();
      // We only need the list of previous operations if we are recording
      // mapping dependences for this fence
      if (!execution)
      {
        // Mapping analysis only
        AutoLock child_lock(child_op_lock,1,false/*exclusive*/);
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executing_children.begin(); it != executing_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_mapping_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_operations.insert(*it);
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executed_children.begin(); it != executed_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_mapping_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_operations.insert(*it);
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              complete_children.begin(); it != complete_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_mapping_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_operations.insert(*it);
        }
      }
      else if (!mapping)
      {
        // Execution analysis only
        AutoLock child_lock(child_op_lock,1,false/*exclusive*/);
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executing_children.begin(); it != executing_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_execution_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executed_children.begin(); it != executed_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_execution_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              complete_children.begin(); it != complete_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's older than the previous fence then we don't care
          if (op_index < current_execution_fence_index)
            continue;
          // Record a dependence if it didn't come after our fence
          if (op_index < next_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
      }
      else
      {
        // Both mapping and execution analysis at the same time
        AutoLock child_lock(child_op_lock,1,false/*exclusive*/);
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executing_children.begin(); it != executing_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our fence we don't care
          if (op_index >= next_fence_index)
            continue;
          if (op_index >= current_mapping_fence_index)
            previous_operations.insert(*it);
          if (op_index >= current_execution_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executed_children.begin(); it != executed_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our fence we don't care
          if (op_index >= next_fence_index)
            continue;
          if (op_index >= current_mapping_fence_index)
            previous_operations.insert(*it);
          if (op_index >= current_execution_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              complete_children.begin(); it != complete_children.end(); it++)
        {
          if (it->first->get_generation() != it->second)
            continue;
          const size_t op_index = it->first->get_ctx_index();
          // If it's younger than our fence we don't care
          if (op_index >= next_fence_index)
            continue;
          if (op_index >= current_mapping_fence_index)
            previous_operations.insert(*it);
          if (op_index >= current_execution_fence_index)
            previous_events.insert(it->first->get_program_order_event());
        }
      }

      // Now record the dependences
      if (!previous_operations.empty())
      {
        for (std::map<Operation*,GenerationID>::const_iterator it = 
             previous_operations.begin(); it != previous_operations.end(); it++)
          op->register_dependence(it->first, it->second);
      }

#ifdef LEGION_SPY
      // Record a dependence on the previous fence
      if (mapping)
      {
        if (current_mapping_fence != NULL)
          LegionSpy::log_mapping_dependence(get_unique_id(), current_fence_uid,
              0/*index*/, op->get_unique_op_id(), 0/*index*/, TRUE_DEPENDENCE);
        for (std::deque<UniqueID>::const_iterator it = 
              ops_since_last_fence.begin(); it != 
              ops_since_last_fence.end(); it++)
        {
          // Skip ourselves if we are here
          if ((*it) == op->get_unique_op_id())
            continue;
          LegionSpy::log_mapping_dependence(get_unique_id(), *it, 0/*index*/,
              op->get_unique_op_id(), 0/*index*/, TRUE_DEPENDENCE); 
        }
      }
      // If we're doing execution record dependence on all previous operations
      if (execution)
      {
        previous_events.insert(previous_completion_events.begin(),
                               previous_completion_events.end());
        // Don't include ourselves though
        previous_events.erase(op->get_program_order_event());
      }
#endif
      // Also include the current execution fence in case the operation
      // already completed and wasn't in the set, make sure to do this
      // before we update the current fence
      if (execution && current_execution_fence_event.exists())
        previous_events.insert(current_execution_fence_event);
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    RefinementOp* InnerContext::get_refinement_op(const LogicalUser &user,
                                                  RegionTreeNode *node)
#else
    RefinementOp* InnerContext::get_refinement_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      return runtime->get_available_refinement_op();
    }

    //--------------------------------------------------------------------------
    void InnerContext::update_current_fence(FenceOp *op, 
                                            bool mapping, bool execution)
    //--------------------------------------------------------------------------
    {
      if (mapping)
      {
        if (current_mapping_fence != NULL)
          current_mapping_fence->remove_mapping_reference(mapping_fence_gen);
        current_mapping_fence = op;
        mapping_fence_gen = op->get_generation();
        current_mapping_fence_index = op->get_ctx_index();
        current_mapping_fence->add_mapping_reference(mapping_fence_gen);
#ifdef LEGION_SPY
        current_fence_uid = op->get_unique_op_id();
        ops_since_last_fence.clear();
#endif
      }
      if (execution)
      {
        // Only update the current fence event if we're actually an
        // execution fence, otherwise by definition we need the previous event
        current_execution_fence_event = op->get_completion_event();
        current_execution_fence_index = op->get_ctx_index();
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::update_current_implicit(Operation *op)
    //--------------------------------------------------------------------------
    {
      // Just overwrite since we know we already recorded a dependence
      // between this operation and the previous last deppart op
      last_implicit = op;
      last_implicit_gen = op->get_generation();
    }

    //--------------------------------------------------------------------------
    RtEvent InnerContext::get_current_mapping_fence_event(void)
    //--------------------------------------------------------------------------
    {
      if (current_mapping_fence == NULL)
        return RtEvent::NO_RT_EVENT;
      RtEvent result = current_mapping_fence->get_mapped_event();
      // Check the generation
      if (current_mapping_fence->get_generation() == mapping_fence_gen)
        return result;
      else
        return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    ApEvent InnerContext::get_current_execution_fence_event(void)
    //--------------------------------------------------------------------------
    {
      return current_execution_fence_event;
    }

    //--------------------------------------------------------------------------
    void InnerContext::begin_trace(TraceID tid, bool logical_only,
        bool static_trace, const std::set<RegionTreeID> *trees, bool deprecated)
    //--------------------------------------------------------------------------
    {
      if (runtime->no_tracing) return;
      if (runtime->no_physical_tracing) logical_only = true;

      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Beginning a trace in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // No need to hold the lock here, this is only ever called
      // by the one thread that is running the task.
      if (current_trace != NULL)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_NESTED_TRACE,
          "Illegal nested trace with ID %d attempted in task %s (ID %lld)", 
          tid, get_task_name(), get_unique_id())
      std::map<TraceID,LegionTrace*>::const_iterator finder = traces.find(tid);
      LegionTrace *trace = NULL;
      if (finder == traces.end())
      {
        // Trace does not exist yet, so make one and record it
        if (static_trace)
          trace = new StaticTrace(tid, this, logical_only, trees);
        else
          trace = new DynamicTrace(tid, this, logical_only);
        if (!deprecated)
          traces[tid] = trace;
        trace->add_reference();
      }
      else
        trace = finder->second;

#ifdef DEBUG_LEGION
      assert(trace != NULL);
#endif
      trace->clear_blocking_call();

      // Issue a begin op
      TraceBeginOp *begin = runtime->get_available_begin_op();
      begin->initialize_begin(this, trace);
      add_to_dependence_queue(begin);

      if (!logical_only)
      {
        // Issue a replay op
        TraceReplayOp *replay = runtime->get_available_replay_op();
        replay->initialize_replay(this, trace);
        add_to_dependence_queue(replay);
      }

      // Now mark that we are starting a trace
      current_trace = trace;
    }

    //--------------------------------------------------------------------------
    void InnerContext::end_trace(TraceID tid, bool deprecated)
    //--------------------------------------------------------------------------
    {
      if (runtime->no_tracing) return;

      AutoRuntimeCall call(this);
#ifdef DEBUG_LEGION
      log_run.debug("Ending a trace in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      if (current_trace == NULL)
        REPORT_LEGION_ERROR(ERROR_UMATCHED_END_TRACE,
          "Unmatched end trace for ID %d in task %s "
                       "(ID %lld)", tid, get_task_name(),
                       get_unique_id())
      else if (!deprecated && (current_trace->tid != tid))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_END_TRACE_CALL,
          "Illegal end trace call on trace ID %d that does not match "
          "the current trace ID %d in task %s (UID %lld)", tid,
          current_trace->tid, get_task_name(), get_unique_id())
      bool has_blocking_call = current_trace->has_blocking_call();
      if (current_trace->is_fixed())
      {
        // Already fixed, dump a complete trace op into the stream
        TraceCompleteOp *complete_op = runtime->get_available_trace_op();
        complete_op->initialize_complete(this, has_blocking_call);
        add_to_dependence_queue(complete_op);
      }
      else
      {
        // Not fixed yet, dump a capture trace op into the stream
        TraceCaptureOp *capture_op = runtime->get_available_capture_op(); 
        capture_op->initialize_capture(this, has_blocking_call, deprecated);
        // Mark that the current trace is now fixed
        current_trace->fix_trace();
        add_to_dependence_queue(capture_op);
      }
      // We no longer have a trace that we're executing 
      current_trace = NULL;
    }

    //--------------------------------------------------------------------------
    void InnerContext::record_previous_trace(LegionTrace *trace)
    //--------------------------------------------------------------------------
    {
      previous_trace = trace;
    }

    //--------------------------------------------------------------------------
    void InnerContext::invalidate_trace_cache(
                                     LegionTrace *trace, Operation *invalidator)
    //--------------------------------------------------------------------------
    {
      if ((previous_trace != NULL) && (previous_trace != trace))
        previous_trace->invalidate_trace_cache(invalidator);
    }

    //--------------------------------------------------------------------------
    void InnerContext::record_blocking_call(void)
    //--------------------------------------------------------------------------
    {
      if (current_trace != NULL)
        current_trace->record_blocking_call();
    }

    //--------------------------------------------------------------------------
    void InnerContext::issue_frame(FrameOp *frame, ApEvent frame_termination)
    //--------------------------------------------------------------------------
    {
      // This happens infrequently enough that we can just issue
      // a meta-task to see what we should do without holding the lock
      if (context_configuration.max_outstanding_frames > 0)
      {
        IssueFrameArgs args(owner_task, this, frame, frame_termination);
        // We know that the issuing is done in order because we block after
        // we launch this meta-task which blocks the application task
        RtEvent wait_on = runtime->issue_runtime_meta_task(args,
                                      LG_LATENCY_WORK_PRIORITY);
        wait_on.wait();
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::perform_frame_issue(FrameOp *frame,
                                         ApEvent frame_termination)
    //--------------------------------------------------------------------------
    {
      ApEvent wait_on, previous;
      {
        AutoLock child_lock(child_op_lock);
        const size_t current_frames = frame_events.size();
        if (current_frames > 0)
          previous = frame_events.back();
        if (current_frames > 
            (size_t)context_configuration.max_outstanding_frames)
          wait_on = frame_events[current_frames - 
                                 context_configuration.max_outstanding_frames];
        frame_events.push_back(frame_termination); 
      }
      frame->set_previous(previous);
      bool poisoned = false;
      if (!wait_on.has_triggered_faultaware(poisoned))
        wait_on.wait_faultaware(poisoned);
      if (poisoned)
        raise_poison_exception();
    }

    //--------------------------------------------------------------------------
    void InnerContext::finish_frame(ApEvent frame_termination)
    //--------------------------------------------------------------------------
    {
      // Pull off all the frame events until we reach ours
      if (context_configuration.max_outstanding_frames > 0)
      {
        AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
        assert(frame_events.front() == frame_termination);
#endif
        frame_events.pop_front();
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::increment_outstanding(void)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert((context_configuration.min_tasks_to_schedule == 0) || 
             (context_configuration.min_frames_to_schedule == 0));
      assert((context_configuration.min_tasks_to_schedule > 0) || 
             (context_configuration.min_frames_to_schedule > 0));
#endif
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
        if (!currently_active_context && (outstanding_subtasks == 0) && 
            (((context_configuration.min_tasks_to_schedule > 0) && 
              (pending_subtasks < 
               context_configuration.min_tasks_to_schedule)) ||
             ((context_configuration.min_frames_to_schedule > 0) &&
              (pending_frames < 
               context_configuration.min_frames_to_schedule))))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = true;
        }
        outstanding_subtasks++;
      }
      if (to_trigger.exists())
      {
        wait_on.wait();
        runtime->activate_context(this);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::decrement_outstanding(void)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert((context_configuration.min_tasks_to_schedule == 0) || 
             (context_configuration.min_frames_to_schedule == 0));
      assert((context_configuration.min_tasks_to_schedule > 0) || 
             (context_configuration.min_frames_to_schedule > 0));
#endif
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
        assert(outstanding_subtasks > 0);
#endif
        outstanding_subtasks--;
        if (currently_active_context && (outstanding_subtasks == 0) && 
            (((context_configuration.min_tasks_to_schedule > 0) &&
              (pending_subtasks < 
               context_configuration.min_tasks_to_schedule)) ||
             ((context_configuration.min_frames_to_schedule > 0) &&
              (pending_frames < 
               context_configuration.min_frames_to_schedule))))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = false;
        }
      }
      if (to_trigger.exists())
      {
        wait_on.wait();
        runtime->deactivate_context(this);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::increment_pending(void)
    //--------------------------------------------------------------------------
    {
      // Don't need to do this if we are scheduling based on mapped frames
      if (context_configuration.min_tasks_to_schedule == 0)
        return;
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
        pending_subtasks++;
        if (currently_active_context && (outstanding_subtasks > 0) &&
            (pending_subtasks == context_configuration.min_tasks_to_schedule))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = false;
        }
      }
      if (to_trigger.exists())
      {
        wait_on.wait();
        runtime->deactivate_context(this);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
    RtEvent InnerContext::decrement_pending(TaskOp *child)
    //--------------------------------------------------------------------------
    {
      // Don't need to do this if we are scheduled by frames
      if (context_configuration.min_tasks_to_schedule == 0)
        return RtEvent::NO_RT_EVENT;
      return decrement_pending(true/*need deferral*/);
    }

    //--------------------------------------------------------------------------
    RtEvent InnerContext::decrement_pending(bool need_deferral)
    //--------------------------------------------------------------------------
    {
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
        assert(pending_subtasks > 0);
#endif
        pending_subtasks--;
        if (!currently_active_context && (outstanding_subtasks > 0) &&
            (pending_subtasks < context_configuration.min_tasks_to_schedule))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = true;
        }
      }
      // Do anything that we need to do
      if (to_trigger.exists())
      {
        if (need_deferral)
        {
          PostDecrementArgs post_decrement_args(this);
          RtEvent done = runtime->issue_runtime_meta_task(post_decrement_args,
              LG_LATENCY_WORK_PRIORITY, wait_on); 
          Runtime::trigger_event(to_trigger, done);
          return to_trigger;
        }
        else
        {
          wait_on.wait();
          runtime->activate_context(this);
          Runtime::trigger_event(to_trigger);
        }
      }
      return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    void InnerContext::increment_frame(void)
    //--------------------------------------------------------------------------
    {
      // Don't need to do this if we are scheduling based on mapped tasks
      if (context_configuration.min_frames_to_schedule == 0)
        return;
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
        pending_frames++;
        if (currently_active_context && (outstanding_subtasks > 0) &&
            (pending_frames == context_configuration.min_frames_to_schedule))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = false;
        }
      }
      if (to_trigger.exists())
      {
        wait_on.wait();
        runtime->deactivate_context(this);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::decrement_frame(void)
    //--------------------------------------------------------------------------
    {
      // Don't need to do this if we are scheduling based on mapped tasks
      if (context_configuration.min_frames_to_schedule == 0)
        return;
      RtEvent wait_on;
      RtUserEvent to_trigger;
      {
        AutoLock child_lock(child_op_lock);
#ifdef DEBUG_LEGION
        assert(pending_frames > 0);
#endif
        pending_frames--;
        if (!currently_active_context && (outstanding_subtasks > 0) &&
            (pending_frames < context_configuration.min_frames_to_schedule))
        {
          wait_on = context_order_event;
          to_trigger = Runtime::create_rt_user_event();
          context_order_event = to_trigger;
          currently_active_context = true;
        }
      }
      if (to_trigger.exists())
      {
        wait_on.wait();
        runtime->activate_context(this);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    MergeCloseOp* InnerContext::get_merge_close_op(const LogicalUser &user,
                                                   RegionTreeNode *node)
#else
    MergeCloseOp* InnerContext::get_merge_close_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      return runtime->get_available_merge_close_op();
    }

    //--------------------------------------------------------------------------
    void InnerContext::record_dynamic_collective_contribution(
                                          DynamicCollective dc, const Future &f) 
    //--------------------------------------------------------------------------
    {
      AutoLock col_lock(collective_lock);
      collective_contributions[dc.phase_barrier].push_back(f);
    }

    //--------------------------------------------------------------------------
    void InnerContext::find_collective_contributions(DynamicCollective dc, 
                                             std::vector<Future> &contributions)
    //--------------------------------------------------------------------------
    {
      // Find any future contributions and record dependences for the op
      // Contributions were made to the previous phase
      ApEvent previous = Runtime::get_previous_phase(dc.phase_barrier);
      AutoLock col_lock(collective_lock);
      std::map<ApEvent,std::vector<Future> >::iterator finder = 
          collective_contributions.find(previous);
      if (finder == collective_contributions.end())
        return;
      contributions = finder->second;
      collective_contributions.erase(finder);
    }

    //--------------------------------------------------------------------------
    ShardingFunction* InnerContext::find_sharding_function(ShardingID sid)
    //--------------------------------------------------------------------------
    {
      // Should only be called by inherited classes
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
    TaskPriority InnerContext::get_current_priority(void) const
    //--------------------------------------------------------------------------
    {
      return current_priority;
    }

    //--------------------------------------------------------------------------
    void InnerContext::set_current_priority(TaskPriority priority)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(mutable_priority);
      assert(realm_done_event.exists());
#endif
      // This can be racy but that is the mappers problem
      realm_done_event.set_operation_priority(priority);
      current_priority = priority;
    }

    //--------------------------------------------------------------------------
    void InnerContext::configure_context(MapperManager *mapper, TaskPriority p)
    //--------------------------------------------------------------------------
    {
      mapper->invoke_configure_context(owner_task, &context_configuration);
      // Do a little bit of checking on the output.  Make
      // sure that we only set one of the two cases so we
      // are counting by frames or by outstanding tasks.
      if ((context_configuration.min_tasks_to_schedule == 0) && 
          (context_configuration.min_frames_to_schedule == 0))
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from call 'configure_context' "
                      "on mapper %s. One of 'min_tasks_to_schedule' and "
                      "'min_frames_to_schedule' must be non-zero for task "
                      "%s (ID %lld)", mapper->get_mapper_name(),
                      get_task_name(), get_unique_id())
      // Hysteresis percentage is an unsigned so can't be less than 0
      if (context_configuration.hysteresis_percentage > 100)
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from call 'configure_context' "
                      "on mapper %s. The 'hysteresis_percentage' %d is not "
                      "a value between 0 and 100 for task %s (ID %lld)",
                      mapper->get_mapper_name(), 
                      context_configuration.hysteresis_percentage,
                      get_task_name(), get_unique_id())
      if (context_configuration.meta_task_vector_width == 0)
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from call 'configure context' "
                      "on mapper %s for task %s (ID %lld). The "
                      "'meta_task_vector_width' must be a non-zero value.",
                      mapper->get_mapper_name(),
                      get_task_name(), get_unique_id())
      if (context_configuration.max_templates_per_trace == 0)
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from call 'configure context' "
                      "on mapper %s for task %s (ID %lld). The "
                      "'max_templates_per_trace' must be a non-zero value.",
                      mapper->get_mapper_name(),
                      get_task_name(), get_unique_id())

      // If we're counting by frames set min_tasks_to_schedule to zero
      if (context_configuration.min_frames_to_schedule > 0)
        context_configuration.min_tasks_to_schedule = 0;
      // otherwise we know min_frames_to_schedule is zero

      // See if we permit priority mutations from child operation mapppers
      mutable_priority = context_configuration.mutable_priority;
      current_priority = p;
    }

    //--------------------------------------------------------------------------
    void InnerContext::initialize_region_tree_contexts(
                      const std::vector<RegionRequirement> &clone_requirements,
                      const LegionVector<VersionInfo>::aligned &version_infos,
                      const std::vector<EquivalenceSet*> &equivalence_sets,
                      const std::vector<ApUserEvent> &unmap_events,
                      std::set<RtEvent> &applied_events,
                      std::set<RtEvent> &execution_events)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, INITIALIZE_REGION_TREE_CONTEXTS_CALL);
      // Save to cast to single task here because this will never
      // happen during inlining of index space tasks
#ifdef DEBUG_LEGION
      assert(owner_task != NULL);
#endif
      const std::deque<InstanceSet> &physical_instances = 
        owner_task->get_physical_instances();
      const std::vector<bool> &no_access_regions = 
        owner_task->get_no_access_regions();
#ifdef DEBUG_LEGION
      assert(regions.size() <= physical_instances.size());
      assert(regions.size() <= virtual_mapped.size());
      assert(regions.size() <= no_access_regions.size());
#endif
      // Initialize all of the logical contexts no matter what
      //
      // For all of the physical contexts that were mapped, initialize them
      // with a specified reference to the current instance, otherwise
      // they were a virtual reference and we can ignore it.
      const UniqueID context_uid = get_unique_id();
      std::map<PhysicalManager*,InstanceView*> top_views;
      const ContextID ctx = tree_context.get_id();
      for (unsigned idx1 = 0; idx1 < regions.size(); idx1++)
      {
#ifdef DEBUG_LEGION
        // this better be true for single tasks
        assert(regions[idx1].handle_type == LEGION_SINGULAR_PROJECTION);
#endif
        // If this is a NO_ACCESS or had no privilege fields we can skip this
        if (no_access_regions[idx1])
          continue;
        EquivalenceSet *eq_set = equivalence_sets[idx1];
        const RegionRequirement &req = clone_requirements[idx1];
        const RegionUsage usage(req);
#ifdef DEBUG_LEGION
        assert(req.handle_type == LEGION_SINGULAR_PROJECTION);
        assert(eq_set != NULL);
        assert(eq_set->region_node->handle == req.region);
#endif
        RegionNode *region_node = eq_set->region_node;
        const FieldMask user_mask = 
          region_node->column_source->get_field_mask(req.privilege_fields);
        // Only need to initialize the context if this is
        // not a leaf and it wasn't virtual mapped
        if (!virtual_mapped[idx1])
        {
          const InstanceSet &sources = physical_instances[idx1];
#ifdef DEBUG_LEGION
          assert(!sources.empty());
#endif
          // Find or make views for each of our instances and then 
          // add initial users for each of them
          std::vector<InstanceView*> corresponding(sources.size());
          // Build our set of corresponding views
          if (IS_REDUCE(req))
          {
            for (unsigned idx2 = 0; idx2 < sources.size(); idx2++)
            {
              const InstanceRef &src_ref = sources[idx2];
              PhysicalManager *manager = src_ref.get_instance_manager();
              const FieldMask &view_mask = src_ref.get_valid_fields();
#ifdef DEBUG_LEGION
              assert(!(view_mask - user_mask)); // should be dominated
#endif
              // Check to see if the view exists yet or not
              std::map<PhysicalManager*,InstanceView*>::const_iterator 
                finder = top_views.find(manager);
              if (finder == top_views.end())
              {
                ReductionView *new_view = create_instance_top_view(manager, 
                    runtime->address_space)->as_reduction_view();
                top_views[manager] = new_view;
                corresponding[idx2] = new_view;
                // Record the initial user for the instance
                new_view->add_initial_user(unmap_events[idx1], usage, view_mask,
                                    region_node->row_source, context_uid, idx1);
              }
              else
              {
                corresponding[idx2] = finder->second;
                // Record the initial user for the instance
                finder->second->add_initial_user(unmap_events[idx1], usage,
                     view_mask, region_node->row_source, context_uid, idx1);
              }
            }
          }
          else
          {
            for (unsigned idx2 = 0; idx2 < sources.size(); idx2++)
            {
              const InstanceRef &src_ref = sources[idx2];
              PhysicalManager *manager = src_ref.get_instance_manager();
              const FieldMask &view_mask = src_ref.get_valid_fields();
#ifdef DEBUG_LEGION
              assert(!(view_mask - user_mask)); // should be dominated
#endif
              // Check to see if the view exists yet or not
              std::map<PhysicalManager*,InstanceView*>::const_iterator 
                finder = top_views.find(manager);
              if (finder == top_views.end())
              {
                MaterializedView *new_view = 
                 create_instance_top_view(manager, 
                     runtime->address_space)->as_materialized_view();
                top_views[manager] = new_view;
                corresponding[idx2] = new_view;
                // Record the initial user for the instance
                new_view->add_initial_user(unmap_events[idx1], usage, view_mask,
                                   region_node->row_source, context_uid, idx1);
              }
              else
              {
                corresponding[idx2] = finder->second;
                // Record the initial user for the instance
                finder->second->add_initial_user(unmap_events[idx1], usage,
                     view_mask, region_node->row_source, context_uid, idx1);
              }
            }
          }
          // The parent region requirement is restricted if it is
          // simultaneous or it is reduce-only. Simultaneous is 
          // restricted because of normal Legion coherence semantics.
          // Reduce-only is restricted because we don't issue close
          // operations at the end of a context for reduce-only cases
          // right now so by making it restricted things are eagerly
          // flushed out to the parent task's instance.
          const bool restricted = 
            IS_SIMULT(regions[idx1]) || IS_REDUCE(regions[idx1]);
          eq_set->initialize_set(usage, user_mask, restricted, sources,
                                 corresponding, applied_events);
        }
        else
        {
#ifdef DEBUG_LEGION
          assert(idx1 < version_infos.size());
#endif
          // virtual mapping case, just clone all the equivalence sets
          // into our current one
          const FieldMaskSet<EquivalenceSet> &eq_sets = 
            version_infos[idx1].get_equivalence_sets();
          const AddressSpaceID space = runtime->address_space;
          for (FieldMaskSet<EquivalenceSet>::const_iterator it =
                eq_sets.begin(); it != eq_sets.end(); it++)
            eq_set->clone_from(space, it->first, it->second, 
                         false/*fowrard to owner*/, execution_events, 
                         IS_WRITE(regions[idx1])/*invalidate source overlap*/);
        }
        // Now initialize our logical and physical contexts
        region_node->initialize_disjoint_complete_tree(ctx, user_mask);
        region_node->initialize_versioning_analysis(ctx, eq_set,
                                                    user_mask, applied_events);
        // Each equivalence set here comes with a CONTEXT_REF that we
        // need to remove after we've registered it
        if (eq_set->remove_base_valid_ref(CONTEXT_REF))
          assert(false); // should never hit this
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::invalidate_region_tree_contexts(
                       const bool is_top_level_task, std::set<RtEvent> &applied)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, INVALIDATE_REGION_TREE_CONTEXTS_CALL);
      // Invalidate all our region contexts
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        if (IS_NO_ACCESS(regions[idx]))
          continue;
        RegionNode *node = runtime->forest->get_node(regions[idx].region);
        runtime->forest->invalidate_current_context(tree_context,
                                       false/*users only*/, node);
        // State is copied out by the virtual close ops if this is a
        // virtual mapped region so we invalidate like normal now
        const FieldMask close_mask = 
          node->column_source->get_field_mask(regions[idx].privilege_fields);
        node->invalidate_refinement(tree_context.get_id(), close_mask,
                      true/*self*/, applied, invalidated_refinements, this);
      }
      if (!created_requirements.empty())
        invalidate_created_requirement_contexts(is_top_level_task, applied);
      // Clean up our instance top views
      if (!instance_top_views.empty())
        clear_instance_top_views();
    }

    //--------------------------------------------------------------------------
    void InnerContext::free_region_tree_context(void)
    //--------------------------------------------------------------------------
    {
      // We can release any invalidated views that we were waiting for the
      // applied effects to be performed before releasing the references
      if (!invalidated_refinements.empty())
      {
        for (std::vector<EquivalenceSet*>::const_iterator it =
              invalidated_refinements.begin(); it != 
              invalidated_refinements.end(); it++)
          if ((*it)->remove_base_valid_ref(DISJOINT_COMPLETE_REF))
            delete (*it);
        invalidated_refinements.clear();
      }
      // Now we can free our region tree context
      runtime->free_region_tree_context(tree_context);
    }

    //--------------------------------------------------------------------------
    void InnerContext::invalidate_created_requirement_contexts(
        const bool is_top, std::set<RtEvent> &applied_events, size_t num_shards)
    //--------------------------------------------------------------------------
    {
      std::set<LogicalRegion> invalidated_regions, return_regions;
      const FieldMask all_ones_mask(LEGION_FIELD_MASK_FIELD_ALL_ONES);
      for (std::map<unsigned,RegionRequirement>::const_iterator it = 
            created_requirements.begin(); it != 
            created_requirements.end(); it++)
      {
#ifdef DEBUG_LEGION
        assert(returnable_privileges.find(it->first) !=
                returnable_privileges.end());
#endif
        // See if we're a returnable privilege or not
        if (returnable_privileges[it->first] && !is_top)
        {
#ifdef DEBUG_LEGION
          assert(invalidated_regions.find(it->second.region) == 
                  invalidated_regions.end());
#endif
          return_regions.insert(it->second.region);
        }
        else // Not returning so invalidate the full thing 
        {
#ifdef DEBUG_LEGION
          assert(return_regions.find(it->second.region) == 
                  return_regions.end());
#endif
          if (invalidated_regions.find(it->second.region) ==
              invalidated_regions.end())
          {
            // Little tricky here, this is safe to invaliate the whole
            // tree even if we only had privileges on a field because
            // if we had privileges on the whole region in this context
            // it would have merged the created_requirement and we wouldn't
            // have a non returnable privilege requirement in this context
            RegionNode *node = runtime->forest->get_node(it->second.region);
            runtime->forest->invalidate_current_context(tree_context,
                                          false/*users only*/, node);
            node->invalidate_refinement(tree_context.get_id(), all_ones_mask,
                true/*self*/, applied_events, invalidated_refinements, this);
            invalidated_regions.insert(it->second.region);
          }
        }
      }
      if (!return_regions.empty())
      {
        std::vector<RegionNode*> created_states(return_regions.size());
        unsigned index = 0;
        for (std::set<LogicalRegion>::const_iterator it = 
              return_regions.begin(); it != return_regions.end(); it++)
          created_states[index++] = runtime->forest->get_node(*it);
        InnerContext *parent_ctx = find_parent_context();   
        parent_ctx->receive_created_region_contexts(tree_context,
            created_states, applied_events, num_shards, this);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::receive_created_region_contexts(
          RegionTreeContext ctx, const std::vector<RegionNode*> &created_states,
          std::set<RtEvent> &applied_events, size_t num_shards,
          InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      const ContextID src_ctx = ctx.get_id();
      const ContextID dst_ctx = tree_context.get_id();
      const bool merge = (num_shards > 0);
      for (std::vector<RegionNode*>::const_iterator it = 
            created_states.begin(); it != created_states.end(); it++)
      {
        (*it)->migrate_logical_state(src_ctx, dst_ctx, merge);
        (*it)->migrate_version_state(src_ctx, dst_ctx, applied_events, 
                                     merge, source_context);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::invalidate_region_tree_context(LogicalRegion handle,
     std::set<RtEvent> &applied_events,std::vector<EquivalenceSet*> &to_release)
    //--------------------------------------------------------------------------
    {
      RegionNode *node = runtime->forest->get_node(handle);
      runtime->forest->invalidate_current_context(tree_context,
                                          false/*users only*/, node);
      const FieldMask all_ones_mask(LEGION_FIELD_MASK_FIELD_ALL_ONES);
      node->invalidate_refinement(tree_context.get_id(), all_ones_mask, 
                        true/*self*/, applied_events, to_release, this);
    }

    //--------------------------------------------------------------------------
    void InnerContext::report_leaks_and_duplicates(std::set<RtEvent> &preconds)
    //--------------------------------------------------------------------------
    {
      // If we have any leaked regions, we need to invalidate their contexts
      // before we do anything else, make sure this is done before we start
      // removing valid references
      if (!created_regions.empty())
      {
        std::set<RtEvent> invalidated_events;
        for (std::map<LogicalRegion,unsigned>::const_iterator it =
              created_regions.begin(); it != created_regions.end(); it++)
          invalidate_region_tree_context(it->first, invalidated_events, 
                                         invalidated_refinements);
        if (!invalidated_events.empty())
        {
          const RtEvent wait_on = Runtime::merge_events(invalidated_events);
          if (wait_on.exists() && !wait_on.has_triggered())
            wait_on.wait();
        }
      }
      // Now we can do the base call
      TaskContext::report_leaks_and_duplicates(preconds);
    }

    //--------------------------------------------------------------------------
    void InnerContext::convert_source_views(
                                   const std::vector<PhysicalManager*> &sources,
                                   std::vector<InstanceView*> &source_views)
    //--------------------------------------------------------------------------
    {
      source_views.resize(sources.size());
      std::vector<unsigned> still_needed;
      {
        AutoLock inst_lock(instance_view_lock,1,false/*exclusive*/); 
        for (unsigned idx = 0; idx < sources.size(); idx++)
        {
          // See if we can find it
          PhysicalManager *manager = sources[idx];
          std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
            instance_top_views.find(manager);     
          if (finder != instance_top_views.end())
            source_views[idx] = finder->second;
          else
            still_needed.push_back(idx);
        }
      }
      if (!still_needed.empty())
      {
        const AddressSpaceID local_space = runtime->address_space;
        for (std::vector<unsigned>::const_iterator it = 
              still_needed.begin(); it != still_needed.end(); it++)
        {
          PhysicalManager *manager = sources[*it];
          source_views[*it] = create_instance_top_view(manager, local_space);
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::clear_instance_top_views(void)
    //--------------------------------------------------------------------------
    {
      for (std::map<PhysicalManager*,InstanceView*>::const_iterator it = 
            instance_top_views.begin(); it != instance_top_views.end(); it++)
      {
        if (it->first->is_owner())
          it->first->unregister_active_context(this);
        if (it->second->remove_base_resource_ref(CONTEXT_REF))
          delete (it->second);
      }
      instance_top_views.clear();
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_compute_equivalence_sets_request(
                   Deserializer &derez, Runtime *runtime, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      UniqueID context_uid;
      derez.deserialize(context_uid);
      // This should always be coming back to the owner node so there's no
      // need to defer this is at should always be here
      InnerContext *local_ctx = runtime->find_context(context_uid);
      EqSetTracker *target;
      derez.deserialize(target);
      LogicalRegion handle;
      derez.deserialize(handle);
      RegionNode *region = runtime->forest->get_node(handle);
      FieldMask mask;
      derez.deserialize(mask);
      UniqueID opid;
      derez.deserialize(opid);
      AddressSpaceID original_source;
      derez.deserialize(original_source);
      RtUserEvent ready_event;
      derez.deserialize(ready_event);

      const RtEvent done = local_ctx->compute_equivalence_sets(target, source,
                                          region, mask, opid, original_source);
      Runtime::trigger_event(ready_event, done);
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::remove_remote_references(
                          const std::vector<DistributedCollectable*> &to_remove)
    //--------------------------------------------------------------------------
    {
      for (std::vector<DistributedCollectable*>::const_iterator it = 
            to_remove.begin(); it != to_remove.end(); it++)
        if ((*it)->remove_base_valid_ref(REMOTE_DID_REF))
          delete (*it);
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_remove_remote_references(
                                                               const void *args)
    //--------------------------------------------------------------------------
    {
      const DeferRemoveRemoteReferenceArgs *dargs = 
        (const DeferRemoveRemoteReferenceArgs*)args;
      InnerContext::remove_remote_references(*(dargs->to_remove));
      delete dargs->to_remove;
    }

    //--------------------------------------------------------------------------
    void InnerContext::convert_target_views(const InstanceSet &targets,
                                       std::vector<InstanceView*> &target_views)
    //--------------------------------------------------------------------------
    {
      target_views.resize(targets.size());
      std::vector<unsigned> still_needed;
      {
        AutoLock inst_lock(instance_view_lock,1,false/*exclusive*/); 
        for (unsigned idx = 0; idx < targets.size(); idx++)
        {
          // See if we can find it
          PhysicalManager *manager = targets[idx].get_instance_manager();
          std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
            instance_top_views.find(manager);     
          if (finder != instance_top_views.end())
            target_views[idx] = finder->second;
          else
            still_needed.push_back(idx);
        }
      }
      if (!still_needed.empty())
      {
        const AddressSpaceID local_space = runtime->address_space;
        for (std::vector<unsigned>::const_iterator it = 
              still_needed.begin(); it != still_needed.end(); it++)
        {
          PhysicalManager *manager = targets[*it].get_instance_manager();
          target_views[*it] = create_instance_top_view(manager, local_space);
        }
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::convert_target_views(const InstanceSet &targets,
                                   std::vector<MaterializedView*> &target_views)
    //--------------------------------------------------------------------------
    {
      std::vector<InstanceView*> inst_views(targets.size());
      convert_target_views(targets, inst_views);
      target_views.resize(inst_views.size());
      for (unsigned idx = 0; idx < inst_views.size(); idx++)
        target_views[idx] = inst_views[idx]->as_materialized_view();
    }

    //--------------------------------------------------------------------------
    InstanceView* InnerContext::create_instance_top_view(
                        PhysicalManager *manager, AddressSpaceID request_source)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, CREATE_INSTANCE_TOP_VIEW_CALL);
      // First check to see if we are the owner node for this manager
      // if not we have to send the message there since the context
      // on that node is actually the point of serialization
      if (!manager->is_owner())
      {
        InstanceView *volatile result = NULL;
        RtUserEvent wait_on = Runtime::create_rt_user_event();
        Serializer rez;
        {
          RezCheck z(rez);
          rez.serialize<UniqueID>(get_context_uid());
          rez.serialize(manager->did);
          rez.serialize<InstanceView**>(const_cast<InstanceView**>(&result));
          rez.serialize(wait_on); 
        }
        runtime->send_create_top_view_request(manager->owner_space, rez);
        wait_on.wait();
#ifdef DEBUG_LEGION
        assert(result != NULL); // when we wake up we should have the result
#endif
        return result;
      }
      // Check to see if we already have the 
      // instance, if we do, return it, otherwise make it and save it
      RtEvent wait_on;
      {
        AutoLock inst_lock(instance_view_lock);
        std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
          instance_top_views.find(manager);
        if (finder != instance_top_views.end())
          // We've already got the view, so we are done
          return finder->second;
        // See if someone else is already making it
        std::map<PhysicalManager*,RtUserEvent>::iterator pending_finder =
          pending_top_views.find(manager);
        if (pending_finder == pending_top_views.end())
          // mark that we are making it
          pending_top_views[manager] = RtUserEvent::NO_RT_USER_EVENT;
        else
        {
          // See if we are the first one to follow
          if (!pending_finder->second.exists())
            pending_finder->second = Runtime::create_rt_user_event();
          wait_on = pending_finder->second;
        }
      }
      if (wait_on.exists())
      {
        // Someone else is making it so we just have to wait for it
        wait_on.wait();
        // Retake the lock and read out the result
        AutoLock inst_lock(instance_view_lock, 1, false/*exclusive*/);
        std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
            instance_top_views.find(manager);
#ifdef DEBUG_LEGION
        assert(finder != instance_top_views.end());
#endif
        return finder->second;
      }
      InstanceView *result = 
        manager->create_instance_top_view(this, request_source);
      result->add_base_resource_ref(CONTEXT_REF);
      // Record the result and trigger any user event to signal that the
      // view is ready
      RtUserEvent to_trigger;
      {
        AutoLock inst_lock(instance_view_lock);
#ifdef DEBUG_LEGION
        assert(instance_top_views.find(manager) == 
                instance_top_views.end());
#endif
        instance_top_views[manager] = result;
        std::map<PhysicalManager*,RtUserEvent>::iterator pending_finder =
          pending_top_views.find(manager);
#ifdef DEBUG_LEGION
        assert(pending_finder != pending_top_views.end());
#endif
        to_trigger = pending_finder->second;
        pending_top_views.erase(pending_finder);
      }
      if (to_trigger.exists())
        Runtime::trigger_event(to_trigger);
      return result;
    }

    //--------------------------------------------------------------------------
    FillView* InnerContext::find_or_create_fill_view(FillOp *op, 
                                     std::set<RtEvent> &map_applied_events,
                                     const void *value, const size_t value_size)
    //--------------------------------------------------------------------------
    {
      // Two versions of this method depending on whether we are doing 
      // Legion Spy or not, Legion Spy wants to know exactly which op
      // made each fill view so we can't cache them
      WrapperReferenceMutator mutator(map_applied_events);
#ifndef LEGION_SPY
      // See if we can find this in the cache first
      AutoLock f_lock(fill_view_lock);
      for (std::list<FillView*>::iterator it = 
            fill_view_cache.begin(); it != fill_view_cache.end(); it++)
      {
        if (!(*it)->value->matches(value, value_size))
          continue;
        // Record a reference on it and then return
        FillView *result = (*it);
        // Move it back to the front of the list
        fill_view_cache.erase(it);
        fill_view_cache.push_front(result);
        result->add_base_valid_ref(MAPPING_ACQUIRE_REF, &mutator);
        return result;
      }
      // At this point we have to make it since we couldn't find it
#endif
      DistributedID did = runtime->get_available_distributed_id();
      FillView::FillViewValue *fill_value = 
        new FillView::FillViewValue(value, value_size);
      FillView *fill_view = 
        new FillView(runtime->forest, did, runtime->address_space,
                     fill_value, true/*register now*/
#ifdef LEGION_SPY
                     , op->get_unique_op_id()

#endif
                     );
      fill_view->add_base_valid_ref(MAPPING_ACQUIRE_REF, &mutator);
#ifndef LEGION_SPY
      // Add it to the cache since we're not doing Legion Spy
      fill_view->add_base_valid_ref(CONTEXT_REF, &mutator);
      fill_view_cache.push_front(fill_view);
      if (fill_view_cache.size() > MAX_FILL_VIEW_CACHE_SIZE)
      {
        FillView *oldest = fill_view_cache.back();
        fill_view_cache.pop_back();
        if (oldest->remove_base_valid_ref(CONTEXT_REF))
          delete oldest;
      }
#endif
      return fill_view;
    }

    //--------------------------------------------------------------------------
    void InnerContext::notify_instance_deletion(PhysicalManager *deleted)
    //--------------------------------------------------------------------------
    {
      InstanceView *removed = NULL;
      {
        AutoLock inst_lock(instance_view_lock);
        std::map<PhysicalManager*,InstanceView*>::iterator finder =  
          instance_top_views.find(deleted);
#ifdef DEBUG_LEGION
        assert(finder != instance_top_views.end());
#endif
        removed = finder->second;
        instance_top_views.erase(finder);
      }
      if (removed->remove_base_resource_ref(CONTEXT_REF))
        delete removed;
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_create_top_view_request(
                   Deserializer &derez, Runtime *runtime, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      UniqueID context_uid;
      derez.deserialize(context_uid);
      DistributedID manager_did;
      derez.deserialize(manager_did);
      InstanceView **target;
      derez.deserialize(target);
      RtUserEvent to_trigger;
      derez.deserialize(to_trigger);
      // Get the context first
      RtEvent ctx_ready;
      InnerContext *context = 
        runtime->find_context(context_uid, false, &ctx_ready);
      RtEvent ready;
      PhysicalManager *manager = 
        runtime->find_or_request_instance_manager(manager_did, ready);
      // Nasty deadlock case: if the request came from a different node
      // we have to defer this because we are in the view virtual channel
      // and we might invoke the update virtual channel, but we already
      // know it's possible for the update channel to block waiting on
      // the view virtual channel (paging views), so to avoid the cycle
      // we have to launch a meta-task and record when it is done
      RemoteCreateViewArgs args(context, manager, target, to_trigger, source);
      if (ready.exists())
      {
        if (ctx_ready.exists())
          runtime->issue_runtime_meta_task(args, LG_LATENCY_DEFERRED_PRIORITY,
              Runtime::merge_events(ready, ctx_ready));
        else
          runtime->issue_runtime_meta_task(args, LG_LATENCY_DEFERRED_PRIORITY,
                                           ready);
      }
      else
        runtime->issue_runtime_meta_task(args, LG_LATENCY_DEFERRED_PRIORITY,
                                         ctx_ready);
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_remote_view_creation(const void *args)
    //--------------------------------------------------------------------------
    {
      const RemoteCreateViewArgs *rargs = (const RemoteCreateViewArgs*)args;
      
      InstanceView *result = rargs->proxy_this->create_instance_top_view(
                                                 rargs->manager, rargs->source);
      // Now we can send the response
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(result->did);
        rez.serialize(rargs->target);
        rez.serialize(rargs->to_trigger);
      }
      rargs->proxy_this->runtime->send_create_top_view_response(rargs->source, 
                                                                rez);
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_create_top_view_response(
                                          Deserializer &derez, Runtime *runtime)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      DistributedID result_did;
      derez.deserialize(result_did);
      InstanceView **target;
      derez.deserialize(target);
      RtUserEvent to_trigger;
      derez.deserialize(to_trigger);
      RtEvent ready;
      LogicalView *view = 
        runtime->find_or_request_logical_view(result_did, ready);
      // Have to static cast since it might not be ready
      *target = static_cast<InstanceView*>(view);
      if (ready.exists())
        Runtime::trigger_event(to_trigger, ready);
      else
        Runtime::trigger_event(to_trigger);
    }

    //--------------------------------------------------------------------------
    bool InnerContext::attempt_children_complete(void)
    //--------------------------------------------------------------------------
    {
      AutoLock chil_lock(child_op_lock);
      if (task_executed && executing_children.empty() && 
          executed_children.empty() && !children_complete_invoked)
      {
        children_complete_invoked = true;
        return true;
      }
      return false;
    }

    //--------------------------------------------------------------------------
    bool InnerContext::attempt_children_commit(void)
    //--------------------------------------------------------------------------
    {
      AutoLock child_lock(child_op_lock);
      if (task_executed && executing_children.empty() && 
          executed_children.empty() && complete_children.empty() && 
          !children_commit_invoked)
      {
        children_commit_invoked = true;
        return true;
      }
      return false;
    }

    //--------------------------------------------------------------------------
    const std::vector<PhysicalRegion>& InnerContext::begin_task(
                                                           Legion::Runtime *&rt)
    //--------------------------------------------------------------------------
    {
      // If we have mutable priority we need to save our realm done event
      if (mutable_priority)
        realm_done_event = ApEvent(Processor::get_current_finish_event());
      // Now do the base begin task routine
      return TaskContext::begin_task(rt);
    }

    //--------------------------------------------------------------------------
    void InnerContext::end_task(const void *res, size_t res_size, bool owned,
     PhysicalInstance deferred_result_instance, FutureFunctor *callback_functor,
                       Memory::Kind result_kind, void (*freefunc)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      // See if we have any local regions or fields that need to be deallocated
      std::vector<LogicalRegion> local_regions_to_delete;
      std::map<FieldSpace,std::set<FieldID> > local_fields_to_delete;
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        for (std::map<LogicalRegion,bool>::const_iterator it = 
              local_regions.begin(); it != local_regions.end(); it++)
          if (!it->second)
            local_regions_to_delete.push_back(it->first);
        for (std::map<std::pair<FieldSpace,FieldID>,bool>::const_iterator it =
              local_fields.begin(); it != local_fields.end(); it++)
          if (!it->second)
            local_fields_to_delete[it->first.first].insert(it->first.second);
      }
      if (!local_regions_to_delete.empty())
      {
        for (std::vector<LogicalRegion>::const_iterator it = 
              local_regions_to_delete.begin(); it != 
              local_regions_to_delete.end(); it++)
          destroy_logical_region(*it, false/*unordered*/);
      }
      if (!local_fields_to_delete.empty())
      {
        for (std::map<FieldSpace,std::set<FieldID> >::const_iterator it = 
              local_fields_to_delete.begin(); it !=
              local_fields_to_delete.end(); it++)
        {
          FieldAllocatorImpl *allocator = 
            create_field_allocator(it->first, false/*unordered*/);
          free_fields(allocator, it->first, it->second, false/*unordered*/);
        }
      }
      if (!index_launch_spaces.empty())
      {
        for (std::map<Domain,IndexSpace>::const_iterator it = 
              index_launch_spaces.begin(); it != 
              index_launch_spaces.end(); it++)
          destroy_index_space(it->second, false/*unordered*/, true/*recurse*/);
      }
      if (overhead_tracker != NULL)
      {
        const long long current = Realm::Clock::current_time_in_nanoseconds();
        const long long diff = current - previous_profiling_time;
        overhead_tracker->application_time += diff;
      } 
      // See if there are any runtime warnings to issue
      if (runtime->runtime_warnings)
      {
        if (total_children_count == 0)
        {
          // If there were no sub operations and this wasn't marked a
          // leaf task then signal a warning
          VariantImpl *impl = 
            runtime->find_variant_impl(owner_task->task_id, 
                                       owner_task->get_selected_variant());
          REPORT_LEGION_WARNING(LEGION_WARNING_VARIANT_TASK_NOT_MARKED,
            "Variant %s of task %s (UID %lld) was "
              "not marked as a 'leaf' variant but it didn't execute any "
              "operations. Did you forget the 'leaf' annotation?", 
              impl->get_name(), get_task_name(), get_unique_id());
        }
        else if (!owner_task->is_inner())
        {
          // If this task had sub operations and wasn't marked as inner
          // and made no accessors warn about missing 'inner' annotation
          // First check for any inline accessors that were made
          bool has_accessor = has_inline_accessor;
          if (!has_accessor)
          {
            for (unsigned idx = 0; idx < physical_regions.size(); idx++)
            {
              if (!physical_regions[idx].impl->created_accessor())
                continue;
              has_accessor = true;
              break;
            }
          }
          if (!has_accessor)
          {
            VariantImpl *impl = 
              runtime->find_variant_impl(owner_task->task_id, 
                                         owner_task->get_selected_variant());
            REPORT_LEGION_WARNING(LEGION_WARNING_VARIANT_TASK_NOT_MARKED,
              "Variant %s of task %s (UID %lld) was "
                "not marked as an 'inner' variant but it only launched "
                "operations and did not make any accessors. Did you "
                "forget the 'inner' annotation?",
                impl->get_name(), get_task_name(), get_unique_id());
          }
        }
      }
      // Quick check to make sure the user didn't forget to end a trace
      if (current_trace != NULL)
        REPORT_LEGION_ERROR(ERROR_TASK_FAILED_END_TRACE,
          "Task %s (UID %lld) failed to end trace before exiting!",
                        get_task_name(), get_unique_id()) 
      // Unmap any of our mapped regions before issuing any close operations
      unmap_all_regions(false/*external*/);
      const std::deque<InstanceSet> &physical_instances = 
        owner_task->get_physical_instances();
      // Note that this loop doesn't handle create regions
      // we deal with that case below
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        if (!virtual_mapped[idx])
        {
          // We also don't need to close up read-only instances
          // or reduction-only instances (because they are restricted)
          // so all changes have already been propagated
          if (!IS_WRITE(regions[idx]))
            continue;
#ifdef DEBUG_LEGION
          assert(!physical_instances[idx].empty());
#endif
          PostCloseOp *close_op = 
            runtime->get_available_post_close_op();
          close_op->initialize(this, idx, physical_instances[idx]);
          add_to_dependence_queue(close_op);
        }
        else
        {
          // Make a virtual close op to close up the instance
          VirtualCloseOp *close_op = 
            runtime->get_available_virtual_close_op();
          close_op->initialize(this, idx, regions[idx],
              &(owner_task->get_version_info(idx)));
          add_to_dependence_queue(close_op);
        }
      }
      // Check to see if we have any unordered operations that we need to inject
      {
        AutoLock d_lock(dependence_lock);
        insert_unordered_ops(d_lock, true/*end task*/, false/*progress*/);
        if (!dependence_queue.empty() && !outstanding_dependence)
        {
          outstanding_dependence = true;
          DependenceArgs args(dependence_queue.front(), this);
          runtime->issue_runtime_meta_task(args, 
              LG_THROUGHPUT_WORK_PRIORITY, dependence_precondition);
          dependence_precondition = RtEvent::NO_RT_EVENT;
        }
      }
      TaskContext::end_task(res, res_size, owned, deferred_result_instance,
                            callback_functor, result_kind, freefunc);
    }

    //--------------------------------------------------------------------------
    void InnerContext::post_end_task(FutureInstance *instance,
                                     FutureFunctor *callback_functor,
                                     bool own_callback_functor)
    //--------------------------------------------------------------------------
    {
      // Safe to cast to a single task here because this will never
      // be called while inlining an index space task
      // Handle the future result
      owner_task->handle_future(instance, callback_functor,
                                executing_processor, own_callback_functor);
      // If we weren't a leaf task, compute the conditions for being mapped
      // which is that all of our children are now mapped
      // Also test for whether we need to trigger any of our child
      // complete or committed operations before marking that we
      // are done executing
      bool need_complete = false;
      bool need_commit = false;
      std::set<RtEvent> preconditions;
      std::set<ApEvent> child_completion_events;
      {
        AutoLock child_lock(child_op_lock);
        // Only need to do this for executing and executed children
        // We know that any complete children are done
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executing_children.begin(); it != 
              executing_children.end(); it++)
        {
          preconditions.insert(it->first->get_mapped_event());
        }
        for (std::map<Operation*,GenerationID>::const_iterator it = 
              executed_children.begin(); it != executed_children.end(); it++)
        {
          preconditions.insert(it->first->get_mapped_event());
        }
#ifdef DEBUG_LEGION
        assert(!task_executed);
#endif
        // Now that we know the last registration has taken place we
        // can mark that we are done executing
        task_executed = true;
        if (executing_children.empty() && executed_children.empty())
        {
          if (!children_complete_invoked)
          {
            need_complete = true;
            children_complete_invoked = true;
            for (LegionMap<Operation*,GenerationID,
                  COMPLETE_CHILD_ALLOC>::tracked::const_iterator it =
                 complete_children.begin(); it != complete_children.end(); it++)
              child_completion_events.insert(it->first->get_completion_event());
          }
          if (complete_children.empty() && 
              !children_commit_invoked)
          {
            need_commit = true;
            children_commit_invoked = true;
          }
        }
        if (!preconditions.empty())
          owner_task->handle_post_mapped(false/*deferral*/,
              Runtime::merge_events(preconditions));
        else
          owner_task->handle_post_mapped(false/*deferral*/);
      }
      if (need_complete)
      {
        if (!child_completion_events.empty())
          owner_task->trigger_children_complete(
              Runtime::merge_events(NULL, child_completion_events));
        else
          owner_task->trigger_children_complete(ApEvent::NO_AP_EVENT);
      }
      if (need_commit)
        owner_task->trigger_children_committed();
    }

    //--------------------------------------------------------------------------
    void InnerContext::free_remote_contexts(void)
    //--------------------------------------------------------------------------
    {
      UniqueID local_uid = get_unique_id();
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(local_uid);
      }
      for (std::map<AddressSpaceID,RemoteContext*>::const_iterator it = 
            remote_instances.begin(); it != remote_instances.end(); it++)
      {
        runtime->send_remote_context_free(it->first, rez);
      }
      remote_instances.clear();
    }

    //--------------------------------------------------------------------------
    void InnerContext::send_remote_context(AddressSpaceID remote_instance,
                                           RemoteContext *remote_ctx)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(remote_instance != runtime->address_space);
#endif
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(remote_ctx);
        pack_remote_context(rez, remote_instance);
      }
      runtime->send_remote_context_response(remote_instance, rez);
      AutoLock rem_lock(remote_lock);
#ifdef DEBUG_LEGION
      assert(remote_instances.find(remote_instance) == remote_instances.end());
#endif
      remote_instances[remote_instance] = remote_ctx;
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_prepipeline_stage(const void *args)
    //--------------------------------------------------------------------------
    {
      const PrepipelineArgs *pargs = (const PrepipelineArgs*)args;
      if (pargs->context->process_prepipeline_stage() &&
          pargs->context->remove_reference())
        delete pargs->context;
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_dependence_stage(const void *args)
    //--------------------------------------------------------------------------
    {
      const DependenceArgs *dargs = (const DependenceArgs*)args;
      dargs->context->process_dependence_stage();
    }

    //--------------------------------------------------------------------------
    /*static*/ void InnerContext::handle_post_end_task(const void *args)
    //--------------------------------------------------------------------------
    {
      const PostEndArgs *pargs = (const PostEndArgs*)args;
      if (pargs->proxy_this->process_post_end_tasks() && 
          pargs->proxy_this->remove_reference())
        delete pargs->proxy_this;
    }

    //--------------------------------------------------------------------------
    bool InnerContext::inline_child_task(TaskOp *child)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, INLINE_CHILD_TASK_CALL);
      if (runtime->legion_spy_enabled)
        LegionSpy::log_inline_task(child->get_unique_id());
      // Check to see if the child is predicated
      // If it is wait for it to resolve
      if (child->is_predicated_op())
      {
        // See if the predicate speculates false, if so return false
        // and then we are done.
        if (!child->get_predicate_value(executing_processor))
          return true;
      }
      // Find the mapped physical regions associated with each of the
      // child task's region requirements. If we don't have one then
      // it's not legal to inline the child task
      std::vector<PhysicalRegion> child_regions(child->regions.size());
      for (unsigned childidx = 0; childidx < child_regions.size(); childidx++)
      {
        const RegionRequirement &child_req = child->regions[childidx]; 
        bool found = false;
        for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
        {
          if (!physical_regions[our_idx].is_mapped())
            continue;
          const RegionRequirement &our_req = regions[our_idx];
          const RegionTreeID our_tid = our_req.region.get_tree_id();
          const IndexSpace our_space = our_req.region.get_index_space();
          const RegionUsage our_usage(our_req);
          if (!check_region_dependence(our_tid, our_space, our_req,
                  our_usage, child_req, false/*ignore privileges*/))
            continue;
          child_regions[childidx] = physical_regions[our_idx];
          found = true;
          break;
        }
        if (found)
          continue;
        // Need the lock here because of unordered detach operations
        AutoLock i_lock(inline_lock,1,false/*exclusive*/);
        for (std::list<PhysicalRegion>::const_iterator it =
              inline_regions.begin(); it != inline_regions.end(); it++)
        {
#ifdef DEBUG_LEGION
          assert(it->is_mapped());
#endif
          const RegionRequirement &our_req = it->impl->get_requirement();
          const RegionTreeID our_tid = our_req.region.get_tree_id();
          const IndexSpace our_space = our_req.region.get_index_space();
          const RegionUsage our_usage(our_req);
          if (!check_region_dependence(our_tid, our_space, our_req,
                  our_usage, child_req, false/*ignore privileges*/))
            continue;
          child_regions[childidx] = *it;
          found = true;
          break;
        }
        // If we didn't find any physical region then report the warning
        // and return because we couldn't find a mapped physical region
        if (!found)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_FAILED_INLINING,
              "Failed to inline task %s (UID %lld) into parent task "
              "%s (UID %lld) because there was no mapped region for "
              "region requirement %d to use. Currently all regions "
              "must be mapped in the parent task in order to allow "
              "for inlining. If you believe you have a compelling use "
              "case for inline a task with virtually mapped regions "
              "then please contact the Legion developers.", 
              child->get_task_name(), child->get_unique_id(), 
              owner_task->get_task_name(), owner_task->get_unique_id(),childidx)
          return false;
        }
      }
      register_executing_child(child);
      const ApEvent child_done = child->get_completion_event();
      // Now select the variant for task based on the regions 
      std::deque<InstanceSet> physical_instances(child_regions.size());
      VariantImpl *variant = 
        select_inline_variant(child, child_regions, physical_instances); 
      child->perform_inlining(variant, physical_instances);
      // Then wait for the child operation to be finished
      bool poisoned = false;
      if (!child_done.has_triggered_faultaware(poisoned))
        child_done.wait_faultaware(poisoned);
      if (poisoned)
        raise_poison_exception();
      return true;
    } 

    //--------------------------------------------------------------------------
    void InnerContext::handle_registration_callback_effects(RtEvent effects)
    //--------------------------------------------------------------------------
    {
      if (current_trace != NULL)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_PERFORM_REGISTRATION_CALLBACK,
            "Illegal call to 'perform_registration_callback' performed "
            "inside of a trace by task %s (UID %lld). Calls to "
            "'perform_registration_callback' are only permitted outside "
            "of traces.", get_task_name(), get_unique_id()) 
      if (effects.has_triggered())
        return;
      // Dump a mapping fence into the stream that will not be considered
      // mapped until these effects are done so that we can ensure that 
      // no downstream operations attempt to do anything on remote nodes
      // which could need the results of the registration
      FenceOp *fence_op = runtime->get_available_fence_op(); 
      fence_op->initialize(this, FenceOp::MAPPING_FENCE, false/*need future*/);
      fence_op->add_mapping_applied_condition(effects);
      add_to_dependence_queue(fence_op);
    }

    //--------------------------------------------------------------------------
    void InnerContext::analyze_free_local_fields(FieldSpace handle,
                                     const std::vector<FieldID> &local_to_free,
                                     std::vector<unsigned> &local_field_indexes)
    //--------------------------------------------------------------------------
    {
      AutoLock local_lock(local_field_lock,1,false/*exclusive*/);
      std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator 
        finder = local_field_infos.find(handle);
#ifdef DEBUG_LEGION
      assert(finder != local_field_infos.end());
#endif
      for (unsigned idx = 0; idx < local_to_free.size(); idx++)
      {
#ifdef DEBUG_LEGION
        bool found = false;
#endif
        for (std::vector<LocalFieldInfo>::const_iterator it = 
              finder->second.begin(); it != finder->second.end(); it++)
        {
          if (it->fid == local_to_free[idx])
          {
            // Can't remove it yet
            local_field_indexes.push_back(it->index);
#ifdef DEBUG_LEGION
            found = true;
#endif
            break;
          }
        }
#ifdef DEBUG_LEGION
        assert(found);
#endif
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::remove_deleted_local_fields(FieldSpace space,
                                          const std::vector<FieldID> &to_remove)
    //--------------------------------------------------------------------------
    {
      AutoLock local_lock(local_field_lock);
      std::map<FieldSpace,std::vector<LocalFieldInfo> >::iterator 
        finder = local_field_infos.find(space);
#ifdef DEBUG_LEGION
      assert(finder != local_field_infos.end());
#endif
      for (unsigned idx = 0; idx < to_remove.size(); idx++)
      {
#ifdef DEBUG_LEGION
        bool found = false;
#endif
        for (std::vector<LocalFieldInfo>::iterator it = 
              finder->second.begin(); it != finder->second.end(); it++)
        {
          if (it->fid == to_remove[idx])
          {
            finder->second.erase(it);
#ifdef DEBUG_LEGION
            found = true;
#endif
            break;
          }
        }
#ifdef DEBUG_LEGION
        assert(found);
#endif
      }
      if (finder->second.empty())
        local_field_infos.erase(finder);
    } 

    //--------------------------------------------------------------------------
    void InnerContext::execute_task_launch(TaskOp *task, bool index,
       LegionTrace *current_trace, bool silence_warnings, bool inlining_enabled)
    //--------------------------------------------------------------------------
    {
      bool inline_task = false;
      if (inlining_enabled)
        inline_task = task->select_task_options(true/*prioritize*/);
      // Now check to see if we're inling the task or just performing
      // a normal asynchronous task launch
      if (!inline_task || !inline_child_task(task))
      {
        // Normal task launch, iterate over the context task's
        // regions and see if we need to unmap any of them
        std::vector<PhysicalRegion> unmapped_regions;
        if (!runtime->unsafe_launch)
          find_conflicting_regions(task, unmapped_regions);
        if (!unmapped_regions.empty())
        {
          if (runtime->runtime_warnings && !silence_warnings)
          {
            if (index)
            {
              REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
                "WARNING: Runtime is unmapping and remapping "
                  "physical regions around execute_index_space call in "
                  "task %s (UID %lld).", get_task_name(), get_unique_id());
            }
            else
            {
              REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
                "WARNING: Runtime is unmapping and remapping "
                  "physical regions around execute_task call in "
                  "task %s (UID %lld).", get_task_name(), get_unique_id());
            }
          }
          for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
            unmapped_regions[idx].impl->unmap_region();
        }
        // Issue the task call
        add_to_dependence_queue(task);
        // Remap any unmapped regions
        if (!unmapped_regions.empty())
          remap_unmapped_regions(current_trace, unmapped_regions);
      }
    }

    //--------------------------------------------------------------------------
    void InnerContext::clone_local_fields(
           std::map<FieldSpace,std::vector<LocalFieldInfo> > &child_local) const
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(child_local.empty());
#endif
      AutoLock local_lock(local_field_lock,1,false/*exclusive*/);
      if (local_field_infos.empty())
        return;
      for (std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator
            fit = local_field_infos.begin(); 
            fit != local_field_infos.end(); fit++)
      {
        std::vector<LocalFieldInfo> &child = child_local[fit->first];
        child.resize(fit->second.size());
        for (unsigned idx = 0; idx < fit->second.size(); idx++)
        {
          LocalFieldInfo &field = child[idx];
          field = fit->second[idx];
          field.ancestor = true; // mark that this is an ancestor field
        }
      }
    }

#ifdef DEBUG_LEGION
    //--------------------------------------------------------------------------
    Operation* InnerContext::get_earliest(void) const
    //--------------------------------------------------------------------------
    {
      Operation *result = NULL;
      unsigned index = 0;
      for (std::map<Operation*,GenerationID>::const_iterator it = 
            executing_children.begin(); it != executing_children.end(); it++)
      {
        if (result == NULL)
        {
          result = it->first;
          index = result->get_ctx_index();
        }
        else if (it->first->get_ctx_index() < index)
        {
          result = it->first;
          index = result->get_ctx_index();
        }
      }
      return result;
    }
#endif

#ifdef LEGION_SPY
    //--------------------------------------------------------------------------
    void InnerContext::register_implicit_replay_dependence(Operation *op)
    //--------------------------------------------------------------------------
    {
      LegionSpy::log_mapping_dependence(get_unique_id(), 
          current_fence_uid, 0/*idx*/, op->get_unique_op_id(),
          0/*idx*/, LEGION_TRUE_DEPENDENCE);
    }
#endif

    /////////////////////////////////////////////////////////////
    // Top Level Context 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    TopLevelContext::TopLevelContext(Runtime *rt, UniqueID ctx_id)
      : InnerContext(rt, NULL, -1, false/*full inner*/,
                     dummy_requirements, dummy_output_requirements,
                     dummy_indexes, dummy_mapped, ctx_id, ApEvent::NO_AP_EVENT)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    TopLevelContext::TopLevelContext(const TopLevelContext &rhs)
      : InnerContext(NULL, NULL, -1, false/*full inner*/,
                     dummy_requirements, dummy_output_requirements,
                     dummy_indexes, dummy_mapped, 0, ApEvent::NO_AP_EVENT)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    TopLevelContext::~TopLevelContext(void)
    //--------------------------------------------------------------------------
    { 
    }

    //--------------------------------------------------------------------------
    TopLevelContext& TopLevelContext::operator=(const TopLevelContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    void TopLevelContext::pack_remote_context(Serializer &rez, 
                                          AddressSpaceID target, bool replicate)
    //--------------------------------------------------------------------------
    {
      rez.serialize(depth);
    }

    //--------------------------------------------------------------------------
    InnerContext* TopLevelContext::find_parent_context(void)
    //--------------------------------------------------------------------------
    {
      return NULL;
    }

    //--------------------------------------------------------------------------
    void TopLevelContext::receive_created_region_contexts(RegionTreeContext ctx,
                 const std::vector<RegionNode*> &created_states,
                 std::set<RtEvent> &applied_events, size_t num_shards,
                 InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    RtEvent TopLevelContext::compute_equivalence_sets(EqSetTracker *target,
                      AddressSpaceID target_space, RegionNode *region, 
                      const FieldMask &mask, const UniqueID opid,
                      const AddressSpaceID original_source)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    InnerContext* TopLevelContext::find_outermost_local_context(
                                                         InnerContext *previous)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(previous != NULL);
#endif
      return previous;
    }

    //--------------------------------------------------------------------------
    InnerContext* TopLevelContext::find_top_context(InnerContext *previous)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(previous != NULL);
#endif
      return previous;
    }

    /////////////////////////////////////////////////////////////
    // Replicate Context 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    ReplicateContext::ReplicateContext(Runtime *rt, 
                                 ShardTask *owner, int d, bool full,
                                 const std::vector<RegionRequirement> &reqs,
                                 const std::vector<RegionRequirement> &out_reqs,
                                 const std::vector<unsigned> &parent_indexes,
                                 const std::vector<bool> &virt_mapped,
                                 UniqueID ctx_uid, ApEvent exec_fence,
                                 ShardManager *manager, bool inline_task)
      : InnerContext(rt, owner, d, full, reqs, out_reqs, parent_indexes,
         virt_mapped, ctx_uid, exec_fence, false/*remote*/, inline_task),
        owner_shard(owner), shard_manager(manager),
        total_shards(shard_manager->total_shards),
        next_close_mapped_bar_index(0), next_refinement_ready_bar_index(0),
        next_refinement_mapped_bar_index(0), next_indirection_bar_index(0), 
        next_future_map_bar_index(0), index_space_allocator_shard(0), 
        index_partition_allocator_shard(0), field_space_allocator_shard(0), 
        field_allocator_shard(0), logical_region_allocator_shard(0), 
        dynamic_id_allocator_shard(0), equivalence_set_allocator_shard(0), 
        next_available_collective_index(0), next_logical_collective_index(1),
        next_physical_template_index(0), next_replicate_bar_index(0), 
        next_logical_bar_index(0), unordered_ops_counter(0), 
        unordered_ops_epoch(MIN_UNORDERED_OPS_EPOCH)
    //--------------------------------------------------------------------------
    {
      // Get our allocation barriers
      pending_partition_barrier = manager->get_pending_partition_barrier();
      creation_barrier = manager->get_creation_barrier();
      deletion_ready_barrier = manager->get_deletion_ready_barrier();
      deletion_mapping_barrier = manager->get_deletion_mapping_barrier();
      deletion_execution_barrier = manager->get_deletion_execution_barrier();
      inline_mapping_barrier = manager->get_inline_mapping_barrier();
      external_resource_barrier = manager->get_external_resource_barrier();
      mapping_fence_barrier = manager->get_mapping_fence_barrier();
      resource_return_barrier = manager->get_resource_return_barrier();
      trace_recording_barrier = manager->get_trace_recording_barrier();
      summary_fence_barrier = manager->get_summary_fence_barrier();
      execution_fence_barrier = manager->get_execution_fence_barrier();
      attach_broadcast_barrier = manager->get_attach_broadcast_barrier();
      attach_reduce_barrier = manager->get_attach_reduce_barrier();
      dependent_partition_barrier = manager->get_dependent_partition_barrier();
      semantic_attach_barrier = manager->get_semantic_attach_barrier();
      inorder_barrier = manager->get_inorder_barrier();
#ifdef DEBUG_LEGION_COLLECTIVES
      collective_check_barrier = manager->get_collective_check_barrier();
      logical_check_barrier = manager->get_logical_check_barrier();
      close_check_barrier = manager->get_close_check_barrier();
      refinement_check_barrier = manager->get_refinement_check_barrier();
      collective_guard_reentrant = false;
      logical_guard_reentrant = false;
#endif
      // Configure our collective settings
      shard_collective_radix = runtime->legion_collective_radix;
      configure_collective_settings(total_shards, owner->shard_id,
          shard_collective_radix, shard_collective_log_radix,
          shard_collective_stages, shard_collective_participating_shards,
          shard_collective_last_radix);
    }

    //--------------------------------------------------------------------------
    ReplicateContext::ReplicateContext(const ReplicateContext &rhs)
      : InnerContext(*this), owner_shard(NULL), 
        shard_manager(NULL), total_shards(0)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    ReplicateContext::~ReplicateContext(void)
    //--------------------------------------------------------------------------
    {
      // We delete the barriers that we created
      for (unsigned idx = owner_shard->shard_id; 
            idx < close_mapped_barriers.size(); idx += total_shards)
      {
        Realm::Barrier bar = close_mapped_barriers[idx];
        bar.destroy_barrier();
      }
      for (unsigned idx = owner_shard->shard_id; 
            idx < refinement_ready_barriers.size(); idx += total_shards)
      {
        Realm::Barrier bar = refinement_ready_barriers[idx];
        bar.destroy_barrier();
      }
      for (unsigned idx = owner_shard->shard_id; 
            idx < refinement_mapped_barriers.size(); idx += total_shards)
      {
        Realm::Barrier bar = refinement_mapped_barriers[idx];
        bar.destroy_barrier();
      }
      for (unsigned idx = owner_shard->shard_id;
            idx < indirection_barriers.size(); idx += total_shards)
      {
        Realm::Barrier bar = indirection_barriers[idx];
        bar.destroy_barrier();
      }
      for (unsigned idx = owner_shard->shard_id;
            idx < future_map_barriers.size(); idx += total_shards)
      {
        Realm::Barrier bar = future_map_barriers[idx];
        bar.destroy_barrier();
      }
      while (!pending_index_spaces.empty())
      {
        std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
          pending_index_spaces.front();
        if (collective.second)
        {
          const ISBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_index_space(value.space_id);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        pending_index_spaces.pop_front();
      }
      while (!pending_index_partitions.empty())
      {
        std::pair<ValueBroadcast<IPBroadcast>*,ShardID> &collective = 
          pending_index_partitions.front();
        if (collective.second)
        {
          const IPBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_partition(value.pid);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        pending_index_partitions.pop_front();
      }
      while (!pending_field_spaces.empty())
      {
        std::pair<ValueBroadcast<FSBroadcast>*,bool> &collective = 
          pending_field_spaces.front();
        if (collective.second)
        {
          const FSBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_field_space(value.space_id);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        pending_field_spaces.pop_front();
      }
      while (!pending_fields.empty())
      {
        std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
          pending_fields.front();
        if (!collective.second)
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        pending_fields.pop_front();
      }
      while (!pending_region_trees.empty())
      {
        std::pair<ValueBroadcast<LRBroadcast>*,bool> &collective = 
          pending_region_trees.front();
        if (collective.second)
        {
          const LRBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_region_tree(value.tid);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        pending_region_trees.pop_front();
      }
      if (returned_resource_ready_barrier.exists())
        returned_resource_ready_barrier.destroy_barrier();
      if (returned_resource_mapped_barrier.exists())
        returned_resource_mapped_barrier.destroy_barrier();
      if (returned_resource_execution_barrier.exists())
        returned_resource_execution_barrier.destroy_barrier();
    }

    //--------------------------------------------------------------------------
    ReplicateContext& ReplicateContext::operator=(const ReplicateContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

#ifdef LEGION_USE_LIBDL
    //--------------------------------------------------------------------------
    void ReplicateContext::perform_global_registration_callbacks(
                     Realm::DSOReferenceImplementation *dso, RtEvent local_done,
                     RtEvent global_done, std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_PERFORM_REGISTRATION_CALLBACK);
        hasher.hash(dso->dso_name.c_str(), dso->dso_name.size());
        hasher.hash(dso->symbol_name.c_str(), dso->symbol_name.size());
        verify_replicable(hasher, "perform_registration_callback");
      }
      shard_manager->perform_global_registration_callbacks(dso, local_done, 
                                                global_done, preconditions);
    }
#endif

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_registration_callback_effects(RtEvent effects)
    //--------------------------------------------------------------------------
    {
      if (current_trace != NULL)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_PERFORM_REGISTRATION_CALLBACK,
            "Illegal call to 'perform_registration_callback' performed "
            "inside of a trace by task %s (UID %lld). Calls to "
            "'perform_registration_callback' are only permitted outside "
            "of traces.", get_task_name(), get_unique_id())
      // Dump a mapping fence into the stream that will not be considered
      // mapped until these effects are done so that we can ensure that 
      // no downstream operations attempt to do anything on remote nodes
      // which could need the results of the registration
      ReplFenceOp *fence_op = runtime->get_available_repl_fence_op();
      fence_op->initialize(this, FenceOp::MAPPING_FENCE, false);
      fence_op->add_mapping_applied_condition(effects);
      add_to_dependence_queue(fence_op);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::print_once(FILE *f, const char *message) const
    //--------------------------------------------------------------------------
    {
      // Only print from shard 0
      if (owner_shard->shard_id == 0)
        fprintf(f, "%s", message);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::log_once(Realm::LoggerMessage &message) const
    //--------------------------------------------------------------------------
    {
      // Deactivate all the messages except shard 0
      if (owner_shard->shard_id != 0)
        message.deactivate();
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::from_value(const void *value, size_t size, 
                      bool owned, Memory::Kind kind, void (*func)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      Future result = TaskContext::from_value(value, size, owned, kind, func);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_FUTURE_FROM_VALUE);
        hash_future(hasher, runtime->safe_control_replication, result);
        verify_replicable(hasher, "future_from_value");
      }
      return result;
    }

    //--------------------------------------------------------------------------
    ShardID ReplicateContext::get_shard_id(void) const
    //--------------------------------------------------------------------------
    {
      return owner_shard->shard_id;
    }

    //--------------------------------------------------------------------------
    size_t ReplicateContext::get_num_shards(void) const
    //--------------------------------------------------------------------------
    {
      return total_shards;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::consensus_match(const void *input, void *output,
                                       size_t num_elements, size_t element_size)
    //--------------------------------------------------------------------------
    {
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CONSENSUS_MATCH);
        verify_replicable(hasher, "consensus_match");
      }
      ApUserEvent complete = Runtime::create_ap_user_event(NULL);
      Future result = runtime->help_create_future(complete);
      switch (element_size)
      {
        case 1:
          {
            ConsensusMatchExchange<uint8_t> *collective = 
              new ConsensusMatchExchange<uint8_t>(this, COLLECTIVE_LOC_89,
                                                  result, output, complete);
            if (collective->match_elements_async(input, num_elements))
              delete collective;
            break;
          }
        case 2:
          {
            ConsensusMatchExchange<uint16_t> *collective = 
              new ConsensusMatchExchange<uint16_t>(this, COLLECTIVE_LOC_89,
                                                   result, output, complete);
            if (collective->match_elements_async(input, num_elements))
              delete collective;
            break;
          }
        case 4:
          {
            ConsensusMatchExchange<uint32_t> *collective = 
              new ConsensusMatchExchange<uint32_t>(this, COLLECTIVE_LOC_89,
                                                   result, output, complete);
            if (collective->match_elements_async(input, num_elements))
              delete collective;
            break;
          }
        case 8:
          {
            ConsensusMatchExchange<uint64_t> *collective = 
              new ConsensusMatchExchange<uint64_t>(this, COLLECTIVE_LOC_89,
                                                   result, output, complete);
            if (collective->match_elements_async(input, num_elements))
              delete collective;
            break;
          }
        default:
          REPORT_LEGION_FATAL(LEGION_FATAL_UNSUPPORTED_CONSENSUS_SIZE,
              "Unsupported size %zd for consensus match in %s (UID %lld)",
              element_size, get_task_name(), get_unique_id())
      }
      return result;
    }

    //--------------------------------------------------------------------------
    VariantID ReplicateContext::register_variant(
                const TaskVariantRegistrar &registrar, const void *user_data,
                size_t user_data_size, const CodeDescriptor &desc, bool ret,
                VariantID vid, bool check_task_id)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::register_variant(registrar, user_data, 
            user_data_size, desc, ret, vid, check_task_id);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_REGISTER_TASK_VARIANT);
        hasher.hash(registrar.task_id);
        hasher.hash(registrar.global_registration);
        if (registrar.task_variant_name != NULL)
          hasher.hash(registrar.task_variant_name, 
                      strlen(registrar.task_variant_name));
        Serializer rez;
        registrar.execution_constraints.serialize(rez);
        registrar.layout_constraints.serialize(rez);
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        for (std::set<TaskID>::const_iterator it = 
              registrar.generator_tasks.begin(); it !=
              registrar.generator_tasks.end(); it++)
          hasher.hash(*it);
        hasher.hash(registrar.leaf_variant);
        hasher.hash(registrar.inner_variant);
        hasher.hash(registrar.idempotent_variant);
        hasher.hash(registrar.replicable_variant);
        if ((user_data != NULL) && (runtime->safe_control_replication > 1))
          hasher.hash(user_data, user_data_size);
        hasher.hash(vid);
        verify_replicable(hasher, "register_task_variant");
      }
      VariantID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<VariantID> collective(this, COLLECTIVE_LOC_17);
        // Have this shard do the registration, and then broadcast the
        // resulting variant to all the other shards
        result = runtime->register_variant(registrar, user_data, user_data_size,
                         desc, ret, vid, check_task_id, false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<VariantID> collective(this, dynamic_id_allocator_shard,
                                             COLLECTIVE_LOC_17);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    VariantImpl* ReplicateContext::select_inline_variant(TaskOp *child,
                              const std::vector<PhysicalRegion> &parent_regions,
                              std::deque<InstanceSet> &physical_instances)
    //--------------------------------------------------------------------------
    {
      VariantImpl *variant_impl = TaskContext::select_inline_variant(child,
                                        parent_regions, physical_instances);
      if (!variant_impl->is_replicable())
      {
        MapperManager *child_mapper = 
          runtime->find_mapper(executing_processor, child->map_id);
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from invoction of "
                      "'select_task_variant' on mapper %s. Mapper selected "
                      "an invalid variant ID %d for inlining of task %s "
                      "(UID %lld). Parent task %s (UID %lld) is a control-"
                      "replicated task but mapper selected non-replicable "
                      "variant %d for task %s.",child_mapper->get_mapper_name(),
                      variant_impl->vid, child->get_task_name(), 
                      child->get_unique_id(), owner_task->get_task_name(),
                      owner_task->get_unique_id(), variant_impl->vid,
                      child->get_task_name())
      }
      return variant_impl;
    }

    //--------------------------------------------------------------------------
    TraceID ReplicateContext::generate_dynamic_trace_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_trace_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_TRACE_ID);
        verify_replicable(hasher, "generate_dynamic_trace_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      TraceID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<TraceID> collective(this, COLLECTIVE_LOC_9);
        result = runtime->generate_dynamic_trace_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<TraceID> collective(this, dynamic_id_allocator_shard,
                                           COLLECTIVE_LOC_9);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    MapperID ReplicateContext::generate_dynamic_mapper_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_mapper_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_MAPPER_ID);
        verify_replicable(hasher, "generate_dynamic_mapper_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      MapperID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<MapperID> collective(this, COLLECTIVE_LOC_10);
        result = runtime->generate_dynamic_mapper_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<MapperID> collective(this, dynamic_id_allocator_shard,
                                            COLLECTIVE_LOC_10);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    ProjectionID ReplicateContext::generate_dynamic_projection_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_projection_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_PROJECTION_ID);
        verify_replicable(hasher, "generate_dynamic_projection_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      ProjectionID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<ProjectionID> collective(this, COLLECTIVE_LOC_11);
        result = 
          runtime->generate_dynamic_projection_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<ProjectionID> collective(this,dynamic_id_allocator_shard,
                                                COLLECTIVE_LOC_11);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    ShardingID ReplicateContext::generate_dynamic_sharding_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_sharding_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_SHARDING_ID);
        verify_replicable(hasher, "generate_dynamic_sharding_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      ShardingID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<ShardingID> collective(this, COLLECTIVE_LOC_12);
        result = runtime->generate_dynamic_sharding_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<ShardingID> collective(this,dynamic_id_allocator_shard,
                                              COLLECTIVE_LOC_12);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    TaskID ReplicateContext::generate_dynamic_task_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_task_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_TASK_ID);
        verify_replicable(hasher, "generate_dynamic_task_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      TaskID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<TaskID> collective(this, COLLECTIVE_LOC_13);
        result = runtime->generate_dynamic_task_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<TaskID> collective(this, dynamic_id_allocator_shard,
                                          COLLECTIVE_LOC_13);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    ReductionOpID ReplicateContext::generate_dynamic_reduction_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_reduction_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_REDUCTION_ID);
        verify_replicable(hasher, "generate_dynamic_reduction_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      ReductionOpID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<ReductionOpID> collective(this, COLLECTIVE_LOC_14);
        result = runtime->generate_dynamic_reduction_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<ReductionOpID> collective(this, 
            dynamic_id_allocator_shard, COLLECTIVE_LOC_14);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    CustomSerdezID ReplicateContext::generate_dynamic_serdez_id(void)
    //--------------------------------------------------------------------------
    {
      // If we're inside a registration callback we don't care
      if (inside_registration_callback)
        return TaskContext::generate_dynamic_serdez_id();
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_GENERATE_DYNAMIC_SERDEZ_ID);
        verify_replicable(hasher, "generate_dynamic_serdez_id");
      }
      // Otherwise have one shard make it and broadcast it to everyone else
      CustomSerdezID result;
      if (owner_shard->shard_id == dynamic_id_allocator_shard)
      {
        ValueBroadcast<CustomSerdezID> collective(this, COLLECTIVE_LOC_16);
        result = runtime->generate_dynamic_serdez_id(false/*check context*/);
        collective.broadcast(result);
      }
      else
      {
        ValueBroadcast<CustomSerdezID> collective(this, 
            dynamic_id_allocator_shard, COLLECTIVE_LOC_16);
        result = collective.get_value();
      }
      if (++dynamic_id_allocator_shard == total_shards)
        dynamic_id_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    bool ReplicateContext::perform_semantic_attach(bool &global)
    //--------------------------------------------------------------------------
    {
      if (inside_registration_callback)
        return TaskContext::perform_semantic_attach(global);
      // Before we do anything else here, we need to make sure that all
      // the shards are done reading before we attempt to mutate the value
      Runtime::phase_barrier_arrive(semantic_attach_barrier, 1/*count*/);
      const RtEvent wait_on = semantic_attach_barrier;
      advance_replicate_barrier(semantic_attach_barrier, total_shards);
      // Check to see if we can downgrade this to a local_only update
      if (global && shard_manager->is_total_sharding())
        global = false;
      // Wait until all the reads of the semantic info are done 
      if (wait_on.exists() && !wait_on.has_triggered())
        wait_on.wait();
      if (global)
      {
        // If we're still global then just have shard 0 do this for now
        if (owner_shard->shard_id == 0)
          return true;
        post_semantic_attach();
        return false;
      }
      else
      {
        // See if we're the local shard to perform the attach operation
        if (shard_manager->perform_semantic_attach())
          return true;
        post_semantic_attach(); 
        return false;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::post_semantic_attach(void)
    //--------------------------------------------------------------------------
    {
      if (inside_registration_callback)
        return;
      Runtime::phase_barrier_arrive(semantic_attach_barrier, 1/*count*/);
      const RtEvent wait_on = semantic_attach_barrier;
      advance_replicate_barrier(semantic_attach_barrier, total_shards);
      if (wait_on.exists() && !wait_on.has_triggered())
        wait_on.wait();
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::hash_future(Murmur3Hasher &hasher,
                          const unsigned safe_level, const Future &future) const
    //--------------------------------------------------------------------------
    {
      if (future.impl == NULL)
        return;
      const std::vector<std::pair<size_t,DomainPoint> > &coordinates =
        future.impl->get_future_coordinates();
      if (!coordinates.empty())
      {
        for (std::vector<std::pair<size_t,DomainPoint> >::const_iterator it =
              coordinates.begin(); it != coordinates.end(); it++)
        {
          hasher.hash(it->first);
          for (int idx = 0; idx < it->second.get_dim(); idx++)
            hasher.hash(it->second[idx]);
        }
      }
      else if (safe_level > 1)
      {
        size_t size = 0;
        const void *result = future.impl->get_buffer(executing_processor,
            Memory::SYSTEM_MEM, &size, false/*check*/, true/*silence warn*/);
        hasher.hash(result, size);
      }
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_future_map(Murmur3Hasher &hasher,
                                                      const FutureMap &map)
    //--------------------------------------------------------------------------
    {
      if (map.impl == NULL)
        return;
#ifdef DEBUG_LEGION
      ReplFutureMapImpl *impl = 
        dynamic_cast<ReplFutureMapImpl*>(map.impl);
      assert(impl != NULL);
#else
      ReplFutureMapImpl *impl = 
        static_cast<ReplFutureMapImpl*>(map.impl);
#endif
      hasher.hash(impl->op_ctx_index);
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_index_space_requirements(
          Murmur3Hasher &hasher, const std::vector<IndexSpaceRequirement> &reqs)
    //--------------------------------------------------------------------------
    {
      if (reqs.empty())
        return;
      Serializer rez;
      for (std::vector<IndexSpaceRequirement>::const_iterator it = 
            reqs.begin(); it != reqs.end(); it++)
        ExternalMappable::pack_index_space_requirement(*it, rez);
      hasher.hash(rez.get_buffer(), rez.get_used_bytes());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_region_requirements(
           Murmur3Hasher &hasher, const std::vector<RegionRequirement> &regions)
    //--------------------------------------------------------------------------
    {
      if (regions.empty())
        return;
      Serializer rez;
      for (std::vector<RegionRequirement>::const_iterator it = 
            regions.begin(); it != regions.end(); it++)
        ExternalMappable::pack_region_requirement(*it, rez);
      hasher.hash(rez.get_buffer(), rez.get_used_bytes());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_grants(Murmur3Hasher &hasher,
                                               const std::vector<Grant> &grants)
    //--------------------------------------------------------------------------
    {
      if (grants.empty())
        return;
      Serializer rez;
      for (std::vector<Grant>::const_iterator it = 
            grants.begin(); it != grants.end(); it++)
        ExternalMappable::pack_grant(*it, rez);
      hasher.hash(rez.get_buffer(), rez.get_used_bytes());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_phase_barriers(Murmur3Hasher &hasher,
                                      const std::vector<PhaseBarrier> &barriers)
    //--------------------------------------------------------------------------
    {
      if (barriers.empty())
        return;
      Serializer rez;
      for (std::vector<PhaseBarrier>::const_iterator it = 
            barriers.begin(); it != barriers.end(); it++)
        ExternalMappable::pack_phase_barrier(*it, rez);
      hasher.hash(rez.get_buffer(), rez.get_used_bytes());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_argument(Murmur3Hasher &hasher,
                              unsigned safe_level, const TaskArgument &argument)
    //--------------------------------------------------------------------------
    {
      if (safe_level == 1)
        return;
      if (argument.get_size() > 0)
        hasher.hash(argument.get_ptr(), argument.get_size());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_predicate(Murmur3Hasher &hasher,
                                                     const Predicate &pred)
    //--------------------------------------------------------------------------
    {
      if (pred == Predicate::TRUE_PRED)
        hasher.hash(0);
      else if (pred == Predicate::FALSE_PRED)
        hasher.hash(SIZE_MAX);
      else
        hasher.hash(pred.impl->get_ctx_index());
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::hash_static_dependences(
        Murmur3Hasher &hasher, const std::vector<StaticDependence> *dependences)
    //--------------------------------------------------------------------------
    {
      if ((dependences == NULL) || dependences->empty())
        return;
      Serializer rez;
      for (std::vector<StaticDependence>::const_iterator it = 
            dependences->begin(); it != dependences->end(); it++)
      {
        hasher.hash(it->previous_offset);
        hasher.hash(it->previous_req_index);
        hasher.hash(it->current_req_index);
        hasher.hash(it->dependence_type);
        hasher.hash(it->validates);
        hasher.hash(it->shard_only);
        for (std::set<FieldID>::const_iterator fit = 
              it->dependent_fields.begin(); fit != 
              it->dependent_fields.end(); fit++)
          hasher.hash(*fit);
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::hash_task_launcher(Murmur3Hasher &hasher,
                  const unsigned safe_level, const TaskLauncher &launcher) const
    //--------------------------------------------------------------------------
    {
      hasher.hash(launcher.task_id);
      hash_index_space_requirements(hasher, launcher.index_requirements);
      hash_region_requirements(hasher, launcher.region_requirements);
      for (std::vector<Future>::const_iterator it = 
            launcher.futures.begin(); it != launcher.futures.end(); it++)
        hash_future(hasher, safe_level, *it);
      hash_grants(hasher, launcher.grants);
      hash_phase_barriers(hasher, launcher.wait_barriers);
      hash_phase_barriers(hasher, launcher.arrive_barriers);
      hash_argument(hasher, safe_level, launcher.argument);
      hash_predicate(hasher, launcher.predicate);
      hasher.hash(launcher.map_id);
      hasher.hash(launcher.tag);
      for (int idx = 0; idx < launcher.point.get_dim(); idx++)
        hasher.hash(launcher.point[idx]);
      hasher.hash(launcher.sharding_space);
      hash_future(hasher, safe_level, launcher.predicate_false_future);
      hash_argument(hasher, safe_level, launcher.predicate_false_result);
      hash_static_dependences(hasher, launcher.static_dependences);
      hasher.hash(launcher.enable_inlining);
      hasher.hash(launcher.local_function_task);
      hasher.hash(launcher.independent_requirements);
      hasher.hash(launcher.silence_warnings);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::hash_index_launcher(Murmur3Hasher &hasher,
                   const unsigned safe_level, const IndexTaskLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      hasher.hash(launcher.task_id);
      hasher.hash(launcher.launch_domain);
      hasher.hash(launcher.launch_space);
      hasher.hash(launcher.sharding_space);
      hash_index_space_requirements(hasher, launcher.index_requirements);
      hash_region_requirements(hasher, launcher.region_requirements);
      for (std::vector<Future>::const_iterator it =
            launcher.futures.begin(); it != launcher.futures.end(); it++)
        hash_future(hasher, safe_level, *it);
      for (std::vector<ArgumentMap>::const_iterator it =
            launcher.point_futures.begin(); it != 
            launcher.point_futures.end(); it++)
        hash_future_map(hasher, it->impl->freeze(this));
      hash_grants(hasher, launcher.grants);
      hash_phase_barriers(hasher, launcher.wait_barriers);
      hash_phase_barriers(hasher, launcher.arrive_barriers);
      hash_argument(hasher, safe_level, launcher.global_arg);
      if (launcher.argument_map.impl != NULL)
        hash_future_map(hasher, launcher.argument_map.impl->freeze(this));
      hash_predicate(hasher, launcher.predicate);
      hasher.hash(launcher.must_parallelism);
      hasher.hash(launcher.map_id);
      hasher.hash(launcher.tag);
      hash_future(hasher, safe_level, launcher.predicate_false_future);
      hash_argument(hasher, safe_level, launcher.predicate_false_result);
      hash_static_dependences(hasher, launcher.static_dependences);
      hasher.hash(launcher.enable_inlining);
      hasher.hash(launcher.independent_requirements);
      hasher.hash(launcher.silence_warnings);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::verify_replicable(Murmur3Hasher &hasher,
                                             const char *func_name)
    //--------------------------------------------------------------------------
    {
      uint64_t hash[2];
      hasher.finalize(hash);
      VerifyReplicableExchange exchange(COLLECTIVE_LOC_82, this);
      const VerifyReplicableExchange::ShardHashes &hashes =
        exchange.exchange(hash);
      
      // If all shards had the same hashes then we are done
      if (hashes.size() == 1)
        return;
      const std::pair<uint64_t,uint64_t> key(hash[0],hash[1]);
      const VerifyReplicableExchange::ShardHashes::const_iterator
        finder = hashes.find(key);
#ifdef DEBUG_LEGION
      assert(finder != hashes.end());
#endif
      // See if we are one of the lowest hashes and report the error
      // We'll let the other shards continue to avoid printing out
      // too many error messages, they'll be killed soon enough
      if (finder->second == owner_shard->shard_id)
        REPORT_LEGION_ERROR(ERROR_CONTROL_REPLICATION_VIOLATION,
            "Detected control replication violation when invoking %s in "
            "task %s (UID %lld) on shard %d. The hash summary for the function "
            "does not align with the hash summaries from other call sites.",
            func_name, get_task_name(), get_unique_id(), owner_shard->shard_id)
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::help_complete_future(Future &f,
                               const void *result, size_t result_size, bool own)
    //--------------------------------------------------------------------------
    {
      f.impl->set_local(result, result_size, own);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::invalidate_region_tree_contexts(
                       const bool is_top_level_task, std::set<RtEvent> &applied)
    //--------------------------------------------------------------------------
    {
      // This does mostly the same thing as the InnerContext version
      // but handles created requirements differently since we know
      // that we kept those things in our context
      DETAILED_PROFILER(runtime, INVALIDATE_REGION_TREE_CONTEXTS_CALL);
      // Invalidate all our region contexts
      for (unsigned idx = 0; idx < regions.size(); idx++)
      {
        if (IS_NO_ACCESS(regions[idx]))
          continue;
        RegionNode *node = runtime->forest->get_node(regions[idx].region);
        runtime->forest->invalidate_current_context(tree_context,
                                      false/*users only*/, node);
        // State is copied out by the virtual close ops if this is a
        // virtual mapped region so we invalidate like normal now
        const FieldMask close_mask = 
          node->column_source->get_field_mask(regions[idx].privilege_fields);
        node->invalidate_refinement(tree_context.get_id(), close_mask,
                true/*self*/, applied, invalidated_refinements, this);
      }
      if (!created_requirements.empty())
        invalidate_created_requirement_contexts(is_top_level_task, 
                                                applied, total_shards); 
      // Cannot clear our instance top view references until we are deleted 
      // as we might still need to help out our other sibling shards
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::free_region_tree_context(void)
    //--------------------------------------------------------------------------
    {
      // We know all our sibling shards are done so we can free these now
      if (!instance_top_views.empty())
        clear_instance_top_views();
      InnerContext::free_region_tree_context();
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::receive_created_region_contexts(
           RegionTreeContext ctx, const std::vector<RegionNode*> &created_state,
           std::set<RtEvent> &applied_events, size_t num_shards,
           InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      // If we have the same number of shards flowing back then we can just 
      // accept our portion of this, otherwise its too hard to make everything
      // line up when calling 'finalize_disjoint_complete_sets', so we do the 
      // full all-to-all so everyone has the same state
      if (num_shards == total_shards)
      {
        InnerContext::receive_created_region_contexts(ctx, created_state,
                          applied_events, 0/*no merge*/, source_context);
        return;
      }
      // If we make it down here then we're doing the broadcast to everyone
      Serializer rez;
      rez.serialize(runtime->address_space);
      rez.serialize<size_t>(num_shards);
      rez.serialize<size_t>(created_state.size());
      std::vector<DistributedCollectable*> remove_remote_references;
      for (std::vector<RegionNode*>::const_iterator it = 
            created_state.begin(); it != created_state.end(); it++)
      {
        rez.serialize((*it)->handle);
        (*it)->pack_logical_state(ctx.get_id(), rez, false/*invalidate*/, 
                                  remove_remote_references);
        (*it)->pack_version_state(ctx.get_id(), rez, false/*invalidate*/, 
                applied_events, source_context, remove_remote_references);
      }
      std::set<RtEvent> broadcast_events;
      shard_manager->broadcast_created_region_contexts(owner_shard, rez,
                                                       broadcast_events);
      if (!remove_remote_references.empty())
      {
        RtEvent precondition;
        if (!broadcast_events.empty())
          precondition = Runtime::merge_events(broadcast_events);
        if (precondition.exists() && !precondition.has_triggered())
        {
          std::vector<DistributedCollectable*> *to_remove =
            new std::vector<DistributedCollectable*>();
          to_remove->swap(remove_remote_references);
          DeferRemoveRemoteReferenceArgs args(context_uid, to_remove);
          runtime->issue_runtime_meta_task(args, 
              LG_LATENCY_DEFERRED_PRIORITY, precondition);
          applied_events.insert(precondition);
        }
        else
          InnerContext::remove_remote_references(remove_remote_references);
      }
      else if (!broadcast_events.empty())
        applied_events.insert(broadcast_events.begin(), broadcast_events.end());
      receive_replicate_created_region_contexts(ctx, created_state,
                        applied_events, num_shards, source_context);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::receive_replicate_created_region_contexts(
           RegionTreeContext ctx, const std::vector<RegionNode*> &created_state,
           std::set<RtEvent> &applied_events, size_t num_shards,
           InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      InnerContext::receive_created_region_contexts(ctx, created_state,
                            applied_events, num_shards, source_context);
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space(const Domain &domain, 
                                                    TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE);
        hasher.hash(domain);
        hasher.hash(type_tag);
        verify_replicable(hasher, "create_index_space");
      }
      return create_index_space_replicated(&domain, type_tag); 
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_replicated(
                                         const Domain *domain, TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      // Seed this with the first index space broadcast
      if (pending_index_spaces.empty())
        increase_pending_index_spaces(1/*count*/, false/*double*/);
      IndexSpace handle;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
        pending_index_spaces.front();
      if (collective.second)
      {
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, type_tag);
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        IndexSpaceNode *node = 
          runtime->forest->create_index_space(handle, domain, value.did, 
              false/*notify remote*/, value.expr_id, ApEvent::NO_AP_EVENT,
              creation_barrier, &applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_index_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_index.debug("Creating index space %x in task%s (ID %lld)",
                        handle.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_index_space(handle.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, type_tag);
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        runtime->forest->create_index_space(handle, domain, value.did,
               false/*notify remote*/, value.expr_id, ApEvent::NO_AP_EVENT,
               creation_barrier, &applied);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_index_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_index_space_creation(handle);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_index_spaces(double_buffer ? 
          pending_index_spaces.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_unbound_index_space(TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_UNBOUND_INDEX_SPACE);
        Serializer rez;
        hasher.hash(type_tag);
        verify_replicable(hasher, "create_unbounded_index_space");
      }
      return create_index_space_replicated(NULL, type_tag); 
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::increase_pending_index_spaces(unsigned count,
                                                         bool double_next)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < count; idx++)
      {
        if (owner_shard->shard_id == index_space_allocator_shard)
        {
          const IndexSpaceID space_id = runtime->get_unique_index_space_id(); 
          const DistributedID did = runtime->get_available_distributed_id();
          // We're the owner, so make it locally and then broadcast it
          runtime->forest->record_pending_index_space(space_id);
          runtime->record_pending_distributed_collectable(did);
          // Do our arrival on this generation, should be the last one
          ValueBroadcast<ISBroadcast> *collective = 
            new ValueBroadcast<ISBroadcast>(this, COLLECTIVE_LOC_3);
          collective->broadcast(ISBroadcast(space_id,
                runtime->get_unique_index_tree_id(),
                runtime->get_unique_index_space_expr_id(), did, double_next));
          pending_index_spaces.push_back(
              std::pair<ValueBroadcast<ISBroadcast>*,bool>(collective, true));
        }
        else
        {
          ValueBroadcast<ISBroadcast> *collective = 
            new ValueBroadcast<ISBroadcast>(this, index_space_allocator_shard,
                                            COLLECTIVE_LOC_3);
          register_collective(collective);
          pending_index_spaces.push_back(
              std::pair<ValueBroadcast<ISBroadcast>*,bool>(collective, false));
        }
        index_space_allocator_shard++;
        if (index_space_allocator_shard == total_shards)
          index_space_allocator_shard = 0;
        double_next = false;
      }
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space(const Future &future, 
                                                    TypeTag type_tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE);
        hash_future(hasher, runtime->safe_control_replication, future);
        hasher.hash(type_tag);
        verify_replicable(hasher, "create_index_space");
      }
      // Seed this with the first index space broadcast
      if (pending_index_spaces.empty())
        increase_pending_index_spaces(1/*count*/, false/*double*/);
      IndexSpace handle;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
        pending_index_spaces.front();
      IndexSpaceNode *node = NULL;
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();
      const ApEvent ready = creator_op->get_completion_event();
      if (collective.second)
      {
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, type_tag);
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        node = runtime->forest->create_index_space(handle, NULL, value.did,
                                false/*notify remote*/, value.expr_id, ready,
                                creation_barrier, &applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_index_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_index.debug("Creating index space %x in task%s (ID %lld)",
                        handle.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_index_space(handle.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, type_tag);
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        node = runtime->forest->create_index_space(handle, NULL, value.did,
                                false/*notify remote*/, value.expr_id, ready,
                                creation_barrier, &applied);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      creator_op->initialize_index_space(this, node, future,
          shard_manager->is_first_local_shard(owner_shard), 
          &(shard_manager->get_mapping()));
      add_to_dependence_queue(creator_op);
      delete collective.first;
      pending_index_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_index_space_creation(handle);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_index_spaces(double_buffer ? 
          pending_index_spaces.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space(
                                         const std::vector<DomainPoint> &points)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE);
        for (unsigned idx = 0; idx < points.size(); idx++)
          hasher.hash(points[idx]);
        verify_replicable(hasher, "create_index_space");
      }
      switch (points[0].get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            std::vector<Realm::Point<DIM,coord_t> > \
              realm_points(points.size()); \
            for (unsigned idx = 0; idx < points.size(); idx++) \
              realm_points[idx] = Point<DIM,coord_t>(points[idx]); \
            const DomainT<DIM,coord_t> realm_is( \
                (Realm::IndexSpace<DIM,coord_t>(realm_points))); \
            const Domain bounds(realm_is); \
            return create_index_space_replicated(&bounds, \
                NT_TemplateHelper::encode_tag<DIM,coord_t>()); \
          }
        LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space(
                                               const std::vector<Domain> &rects)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE);
        for (unsigned idx = 0; idx < rects.size(); idx++)
          hasher.hash(rects[idx]);
        verify_replicable(hasher, "create_index_space");
      }
      switch (rects[0].get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            std::vector<Realm::Rect<DIM,coord_t> > realm_rects(rects.size()); \
            for (unsigned idx = 0; idx < rects.size(); idx++) \
              realm_rects[idx] = Rect<DIM,coord_t>(rects[idx]); \
            const DomainT<DIM,coord_t> realm_is( \
                (Realm::IndexSpace<DIM,coord_t>(realm_rects))); \
            const Domain bounds(realm_is); \
            return create_index_space_replicated(&bounds, \
                NT_TemplateHelper::encode_tag<DIM,coord_t>()); \
          }
        LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::union_index_spaces(
                                          const std::vector<IndexSpace> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_UNION_INDEX_SPACES);
        for (std::vector<IndexSpace>::const_iterator it = 
              spaces.begin(); it != spaces.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "union_index_spaces");
      }
      if (spaces.empty())
        return IndexSpace::NO_SPACE;
      bool none_exists = true;
      for (std::vector<IndexSpace>::const_iterator it = 
            spaces.begin(); it != spaces.end(); it++)
      {
        if (none_exists && it->exists())
          none_exists = false;
        if (spaces[0].get_type_tag() != it->get_type_tag())
          REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'union_index_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      }
      if (none_exists)
        return IndexSpace::NO_SPACE;
      // Seed this with the first index space broadcast
      if (pending_index_spaces.empty())
        increase_pending_index_spaces(1/*count*/, false/*double*/);
      IndexSpace handle;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
        pending_index_spaces.front();
      if (collective.second)
      {
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid,spaces[0].get_type_tag());
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        IndexSpaceNode *node = 
          runtime->forest->create_union_space(handle, value.did, spaces, 
            creation_barrier, false/*notify remote*/, value.expr_id, &applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_index_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_index.debug("Creating index space %x in task%s (ID %lld)",
                        handle.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_index_space(handle.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid,spaces[0].get_type_tag());
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        runtime->forest->create_union_space(handle, value.did, spaces, 
            creation_barrier, false/*notify remote*/, value.expr_id, &applied);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_index_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_index_space_creation(handle);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_index_spaces(double_buffer ? 
          pending_index_spaces.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::intersect_index_spaces(
                                          const std::vector<IndexSpace> &spaces)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_INTERSECT_INDEX_SPACES);
        for (std::vector<IndexSpace>::const_iterator it = 
              spaces.begin(); it != spaces.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "intersect_index_spaces");
      }
      if (spaces.empty())
        return IndexSpace::NO_SPACE;
      bool none_exists = true;
      for (std::vector<IndexSpace>::const_iterator it = 
            spaces.begin(); it != spaces.end(); it++)
      {
        if (none_exists && it->exists())
          none_exists = false;
        if (spaces[0].get_type_tag() != it->get_type_tag())
          REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'intersect_index_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      }
      if (none_exists)
        return IndexSpace::NO_SPACE;
      // Seed this with the first index space broadcast
      if (pending_index_spaces.empty())
        increase_pending_index_spaces(1/*count*/, false/*double*/);
      IndexSpace handle;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
        pending_index_spaces.front();
      if (collective.second)
      {
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid,spaces[0].get_type_tag());
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        IndexSpaceNode *node = 
          runtime->forest->create_intersection_space(handle, value.did, spaces,
            creation_barrier, false/*notify remote*/, value.expr_id, &applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_index_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_index.debug("Creating index space %x in task%s (ID %lld)",
                        handle.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_index_space(handle.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid,spaces[0].get_type_tag());
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        runtime->forest->create_intersection_space(handle, value.did, spaces,
          creation_barrier, false/*notify remote*/, value.expr_id, &applied);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_index_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_index_space_creation(handle);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_index_spaces(double_buffer ? 
          pending_index_spaces.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::subtract_index_spaces(
                                              IndexSpace left, IndexSpace right)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_SUBTRACT_INDEX_SPACES);
        hasher.hash(left);
        hasher.hash(right);
        verify_replicable(hasher, "subtract_index_spaces");
      }
      if (!left.exists())
        return IndexSpace::NO_SPACE;
      if (right.exists() && left.get_type_tag() != right.get_type_tag())
        REPORT_LEGION_ERROR(ERROR_DYNAMIC_TYPE_MISMATCH,
                        "Dynamic type mismatch in 'create_difference_spaces' "
                        "performed in task %s (UID %lld)",
                        get_task_name(), get_unique_id())
      // Seed this with the first index space broadcast
      if (pending_index_spaces.empty())
        increase_pending_index_spaces(1/*count*/, false/*double*/);
      IndexSpace handle;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
        pending_index_spaces.front();
      if (collective.second)
      {
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, left.get_type_tag());
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        IndexSpaceNode *node = 
          runtime->forest->create_difference_space(handle, value.did, left,
          right,creation_barrier,false/*notify remote*/,value.expr_id,&applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_index_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_index.debug("Creating index space %x in task%s (ID %lld)",
                        handle.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_index_space(handle.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const ISBroadcast value = collective.first->get_value(false);
        handle = IndexSpace(value.space_id, value.tid, left.get_type_tag());
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        runtime->forest->create_difference_space(handle, value.did, left, right,
             creation_barrier, false/*notify remote*/, value.expr_id, &applied);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_index_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_index_space_creation(handle);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_index_spaces(double_buffer ? 
          pending_index_spaces.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_shared_ownership(IndexSpace handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_SHARED_OWNERSHIP);
        hasher.hash(handle);
        verify_replicable(hasher, "create_shared_ownership");
      }
      if (!handle.exists())
        return;
      // Check to see if this is a top-level index space, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_index_space(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_SHARED_OWNERSHIP,
            "Illegal call to create shared ownership for index space %x in " 
            "task %s (UID %lld) which is not a top-level index space. Legion "
            "only permits top-level index spaces to have shared ownership.", 
            handle.get_id(), get_task_name(), get_unique_id())
      if (shard_manager->is_total_sharding() &&
          shard_manager->is_first_local_shard(owner_shard))
        runtime->create_shared_ownership(handle, true/*total sharding*/);
      else if (owner_shard->shard_id == 0)
        runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<IndexSpace,unsigned>::iterator finder = 
        created_index_spaces.find(handle);
      if (finder != created_index_spaces.end())
        finder->second++;
      else
        created_index_spaces[handle] = 1;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_index_space(IndexSpace handle,
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_INDEX_SPACE);
        hasher.hash(handle);
        hasher.hash(recurse);
        verify_replicable(hasher, "destroy_index_space");
      }
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_index.debug("Destroying index space %x in task %s (ID %lld)", 
                        handle.id, get_task_name(), get_unique_id());
#endif
      // Check to see if this is a top-level index space, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_index_space(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy index space %x in task %s (UID %lld) "
            "which is not a top-level index space. Legion only permits "
            "top-level index spaces to be destroyed.", handle.get_id(),
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      std::vector<IndexPartition> sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexSpace,unsigned>::iterator finder = 
          created_index_spaces.find(handle);
        if (finder == created_index_spaces.end())
        {
          // If we didn't make the index space in this context, just
          // record it and keep going, it will get handled later
          deleted_index_spaces.push_back(std::make_pair(handle,recurse));
          return;
        }
        else
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_index_spaces.erase(finder);
          else
            return;
        }
        if (recurse)
        {
          // Also remove any index partitions for this index space tree
          for (std::map<IndexPartition,unsigned>::iterator it = 
                created_index_partitions.begin(); it !=
                created_index_partitions.end(); /*nothing*/)
          {
            if (it->first.get_tree_id() == handle.get_tree_id()) 
            {
              sub_partitions.push_back(it->first);
#ifdef DEBUG_LEGION
              assert(it->second > 0);
#endif
              if (--it->second == 0)
              {
                std::map<IndexPartition,unsigned>::iterator to_delete = it++;
                created_index_partitions.erase(to_delete);
              }
              else
                it++;
            }
            else
              it++;
          }
        }
      }
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_index_space_deletion(this,handle,sub_partitions,unordered);
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_shared_ownership(IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_SHARED_OWNERSHIP);
        hasher.hash(handle);
        verify_replicable(hasher, "create_shared_ownership");
      }
      if (!handle.exists())
        return;
      if (shard_manager->is_total_sharding() &&
          shard_manager->is_first_local_shard(owner_shard))
        runtime->create_shared_ownership(handle, true/*total sharding*/);
      else if (owner_shard->shard_id == 0)
        runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<IndexPartition,unsigned>::iterator finder = 
        created_index_partitions.find(handle);
      if (finder != created_index_partitions.end())
        finder->second++;
      else
        created_index_partitions[handle] = 1;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_index_partition(IndexPartition handle,
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_INDEX_PARTITION);
        hasher.hash(handle);
        hasher.hash(recurse);
        verify_replicable(hasher, "destroy_index_partition");
      }
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_index.debug("Destroying index partition %x in task %s (ID %lld)", 
                        handle.id, get_task_name(), get_unique_id());
#endif
      std::vector<IndexPartition> sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexPartition,unsigned>::iterator finder = 
          created_index_partitions.find(handle);
        if (finder != created_index_partitions.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_index_partitions.erase(finder);
          else
            return;
          if (recurse)
          {
            // Remove any other partitions that this partition dominates
            for (std::map<IndexPartition,unsigned>::iterator it = 
                  created_index_partitions.begin(); it !=
                  created_index_partitions.end(); /*nothing*/)
            {
              if ((handle.get_tree_id() == it->first.get_tree_id()) &&
                  runtime->forest->is_dominated_tree_only(it->first, handle))
              {
                sub_partitions.push_back(it->first);
#ifdef DEBUG_LEGION
                assert(it->second > 0);
#endif
                if (--it->second == 0)
                {
                  std::map<IndexPartition,unsigned>::iterator to_delete = it++;
                  created_index_partitions.erase(to_delete);
                }
                else
                  it++;
              }
              else
                it++;
            }
          }
        }
        else
        {
          // If we didn't make the partition, record it and keep going
          deleted_index_partitions.push_back(std::make_pair(handle,recurse));
          return;
        }
      }
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_index_part_deletion(this, handle, 
                                         sub_partitions, unordered);
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::increase_pending_partitions(unsigned count,
                                                       bool double_next)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < count; idx++)
      {
        if (owner_shard->shard_id == index_partition_allocator_shard)
        {
          const IndexPartitionID pid = runtime->get_unique_index_partition_id();
          const DistributedID did = runtime->get_available_distributed_id();
          // We're the owner, so make it locally and then broadcast it
          runtime->forest->record_pending_partition(pid);
          runtime->record_pending_distributed_collectable(did);
          // Do our arrival on this generation, should be the last one
          ValueBroadcast<IPBroadcast> *collective = 
            new ValueBroadcast<IPBroadcast>(this, COLLECTIVE_LOC_7);
          collective->broadcast(IPBroadcast(pid, did, double_next));
          pending_index_partitions.push_back(
              std::pair<ValueBroadcast<IPBroadcast>*,ShardID>(collective, 
                                        index_partition_allocator_shard));
        }
        else
        {
          ValueBroadcast<IPBroadcast> *collective = 
           new ValueBroadcast<IPBroadcast>(this,index_partition_allocator_shard,
                                           COLLECTIVE_LOC_7);
          register_collective(collective);
          pending_index_partitions.push_back(
              std::pair<ValueBroadcast<IPBroadcast>*,ShardID>(collective, 
                                        index_partition_allocator_shard));
        }
        index_partition_allocator_shard++;
        if (index_partition_allocator_shard == total_shards)
          index_partition_allocator_shard = 0;
        double_next = false;
      }
    }

    //--------------------------------------------------------------------------
    bool ReplicateContext::create_shard_partition(IndexPartition &pid,
             IndexSpace parent, IndexSpace color_space, PartitionKind part_kind,
             LegionColor partition_color, bool color_generated,
             ValueBroadcast<bool> *disjoint_result/*=NULL*/,
             ApBarrier partition_ready /*=ApBarrier::NO_AP_BARRIER*/)
    //--------------------------------------------------------------------------
    {
      if (pending_index_partitions.empty())
        increase_pending_partitions(1/*count*/, false/*double*/);
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<IPBroadcast>*,ShardID> &collective = 
        pending_index_partitions.front();
      const bool is_owner = (collective.second == owner_shard->shard_id);
      if (is_owner)
      {
        const IPBroadcast value = collective.first->get_value(false);
        pid.id = value.pid;
        double_buffer = value.double_buffer;
        // Have to do our registration before broadcasting
        RtEvent safe_event = runtime->forest->create_pending_partition_shard(
                                           collective.second, this, pid, parent,
                                           color_space, partition_color, 
                                           part_kind,value.did,disjoint_result,
                                           partition_ready.exists() ? 
                                             partition_ready :
                                             pending_partition_barrier,
                                           shard_manager->get_mapping(),
                                           creation_barrier, partition_ready);
        // Broadcast the color if we have to generate it
        if (color_generated)
        {
#ifdef DEBUG_LEGION
          assert(partition_color != INVALID_COLOR); // we should have an ID
#endif
          ValueBroadcast<LegionColor> color_collective(this, COLLECTIVE_LOC_8);
          color_collective.broadcast(partition_color);
        }
        // Signal that we're done our creation
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/, safe_event);
        runtime->forest->revoke_pending_partition(value.pid);
        runtime->revoke_pending_distributed_collectable(value.did);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const IPBroadcast value = collective.first->get_value(false);
        pid.id = value.pid;
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(pid.exists());
#endif
        // If we need a color then we can get that too
        if (color_generated)
        {
          ValueBroadcast<LegionColor> color_collective(this, collective.second,
                                                       COLLECTIVE_LOC_8);
          partition_color = color_collective.get_value();
#ifdef DEBUG_LEGION
          assert(partition_color != INVALID_COLOR);
#endif
        }
        // Do our registration
        RtEvent safe_event = runtime->forest->create_pending_partition_shard(
                                         collective.second, this, pid, parent, 
                                         color_space, partition_color, 
                                         part_kind, value.did, disjoint_result,
                                         partition_ready.exists() ?
                                           partition_ready :
                                           pending_partition_barrier,
                                         shard_manager->get_mapping(),
                                         creation_barrier, partition_ready);
        // Signal that we're done our creation
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/, safe_event);
      }
      // Clean up the collective
      delete collective.first;
      pending_index_partitions.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_partitions(double_buffer ? 
        pending_index_partitions.size() + 1 : 1, double_next && !double_buffer);
      return is_owner;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_equal_partition(
                                                      IndexSpace parent,
                                                      IndexSpace color_space,
                                                      size_t granularity,
                                                      Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_EQUAL_PARTITION);
        hasher.hash(parent);
        hasher.hash(color_space);
        hasher.hash(granularity);
        hasher.hash(color);
        verify_replicable(hasher, "create_equal_partition");
      }
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false; 
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true;
      if (create_shard_partition(pid, parent,color_space,
            LEGION_DISJOINT_COMPLETE_KIND, partition_color, color_generated))
        log_index.debug("Creating equal partition %d with parent index space %x"
                        " in task %s (ID %lld)", pid.id, parent.id,
                        get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_equal_partition(this, pid, granularity);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Trigger the pending partition barrier and advance it
      Runtime::phase_barrier_arrive(pending_partition_barrier, 
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_weights(
                                                IndexSpace parent,
                                                const FutureMap &weights, 
                                                IndexSpace color_space,
                                                size_t granularity, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_WEIGHTS);
        hasher.hash(parent);
        hash_future_map(hasher, weights);
        hasher.hash(color_space);
        hasher.hash(granularity);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_weights");
      }
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true;
      if (create_shard_partition(pid, parent,color_space,
            LEGION_DISJOINT_COMPLETE_KIND, partition_color, color_generated))
        log_index.debug("Creating equal partition %d with parent index space %x"
                        " in task %s (ID %lld)", pid.id, parent.id,
                        get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_weight_partition(this, pid, weights, granularity);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Trigger the pending partition barrier and advance it
      Runtime::phase_barrier_arrive(pending_partition_barrier, 
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_union(
                                          IndexSpace parent,
                                          IndexPartition handle1,
                                          IndexPartition handle2,
                                          IndexSpace color_space,
                                          PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_UNION);
        hasher.hash(parent);
        hasher.hash(handle1);
        hasher.hash(handle2);
        hasher.hash(color_space);
        hasher.hash(kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_union");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
#ifdef DEBUG_LEGION 
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                        "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create "
                        "partition by union!", handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                        "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create "
                        "partition by union!", handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true; 
      // If either partition is aliased the result is aliased
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        // If one of these partitions is aliased then the result is aliased
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (p1->is_disjoint(true/*from app*/))
        {
          IndexPartNode *p2 = runtime->forest->get_node(handle2);
          if (!p2->is_disjoint(true/*from app*/))
          {
            if (kind == LEGION_COMPUTE_KIND)
              kind = LEGION_ALIASED_KIND;
            else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
              kind = LEGION_ALIASED_COMPLETE_KIND;
            else
              kind = LEGION_ALIASED_INCOMPLETE_KIND;
          }
        }
        else
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_ALIASED_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_ALIASED_COMPLETE_KIND;
          else
            kind = LEGION_ALIASED_INCOMPLETE_KIND;
        }
      }
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) || 
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_61);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, kind, 
              partition_color, color_generated, disjoint_result))
        log_index.debug("Creating union partition %d with parent index "
                        "space %x in task %s (ID %lld)", pid.id, parent.id,
                        get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_union_partition(this, pid, handle1, handle2);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier, 
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_intersection(
                                              IndexSpace parent,
                                              IndexPartition handle1,
                                              IndexPartition handle2,
                                              IndexSpace color_space,
                                              PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_INTERSECTION);
        hasher.hash(parent);
        hasher.hash(handle1);
        hasher.hash(handle2);
        hasher.hash(color_space);
        hasher.hash(kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_intersection");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
#ifdef DEBUG_LEGION 
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                        "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create partition by "
                        "intersection!", handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                        "IndexPartition %d is not part of the same "
                        "index tree as IndexSpace %d in create partition by "
                        "intersection!", handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true; 
      // If either partition is disjoint then the result is disjoint
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (!p1->is_disjoint(true/*from app*/))
        {
          IndexPartNode *p2 = runtime->forest->get_node(handle2);
          if (p2->is_disjoint(true/*from app*/))
          {
            if (kind == LEGION_COMPUTE_KIND)
              kind = LEGION_DISJOINT_KIND;
            else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
              kind = LEGION_DISJOINT_COMPLETE_KIND;
            else
              kind = LEGION_DISJOINT_INCOMPLETE_KIND;
          }
        }
        else
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_62);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, kind,
              partition_color, color_generated, disjoint_result))
        log_index.debug("Creating intersection partition %d with parent "
                        "index space %x in task %s (ID %lld)", pid.id, 
                        parent.id, get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_intersection_partition(this, pid, handle1, handle2);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier, 
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards); 
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_intersection(
                                                IndexSpace parent,
                                                IndexPartition partition,
                                                PartitionKind kind, Color color,
                                                bool dominates)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_INTERSECTION);
        hasher.hash(parent);
        hasher.hash(partition);
        hasher.hash(kind);
        hasher.hash(color);
        hasher.hash(dominates);
        verify_replicable(hasher, "create_partition_by_intersection");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
#ifdef DEBUG_LEGION
      if (parent.get_type_tag() != partition.get_type_tag())
        REPORT_LEGION_ERROR(ERROR_INDEXPARTITION_NOT_SAME_INDEX_TREE,
            "IndexPartition %d does not have the same type as the "
            "parent index space %x in task %s (UID %lld)", partition.id,
            parent.id, get_task_name(), get_unique_id())
#endif
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true;
      IndexPartNode *part_node = runtime->forest->get_node(partition);
      // See if we can determine disjointness if we weren't told
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        if (part_node->is_disjoint(true/*from app*/))
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_62);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, part_node->color_space->handle,
            kind, partition_color, color_generated, disjoint_result))
        log_index.debug("Creating intersection partition %d with parent "
                        "index space %x in task %s (ID %lld)", pid.id, 
                        parent.id, get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_intersection_partition(this,pid,partition,dominates);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier, 
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards); 
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_difference(
                                                  IndexSpace parent,
                                                  IndexPartition handle1,
                                                  IndexPartition handle2,
                                                  IndexSpace color_space,
                                                  PartitionKind kind, 
                                                  Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_DIFFERENCE);
        hasher.hash(parent);
        hasher.hash(handle1);
        hasher.hash(handle2);
        hasher.hash(color_space);
        hasher.hash(kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_difference");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
#ifdef DEBUG_LEGION 
      if (parent.get_tree_id() != handle1.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                            "IndexPartition %d is not part of the same "
                            "index tree as IndexSpace %d in create "
                            "partition by difference!",
                            handle1.id, parent.id)
      if (parent.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                            "IndexPartition %d is not part of the same "
                            "index tree as IndexSpace %d in create "
                            "partition by difference!",
                            handle2.id, parent.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      else
        color_generated = true; 
      // If the left-hand-side is disjoint the result is disjoint
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p1 = runtime->forest->get_node(handle1);
        if (p1->is_disjoint(true/*from app*/))
        {
          if (kind == LEGION_COMPUTE_KIND)
            kind = LEGION_DISJOINT_KIND;
          else if (kind == LEGION_COMPUTE_COMPLETE_KIND)
            kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((kind == LEGION_COMPUTE_KIND) || 
          (kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_63);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, kind,
              partition_color, color_generated, disjoint_result))
        log_index.debug("Creating difference partition %d with parent "
                        "index space %x in task %s (ID %lld)", pid.id, 
                        parent.id, get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_difference_partition(this, pid, handle1, handle2);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    Color ReplicateContext::create_cross_product_partitions(
                                              IndexPartition handle1,
                                              IndexPartition handle2,
                                std::map<IndexSpace,IndexPartition> &handles,
                                              PartitionKind kind,
                                              Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_CROSS_PRODUCT_PARTITIONS);
        hasher.hash(handle1);
        hasher.hash(handle2);
        hasher.hash(kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_cross_product_partitions");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, kind)
#ifdef DEBUG_LEGION
      log_index.debug("Creating cross product partitions in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
      if (handle1.get_tree_id() != handle2.get_tree_id())
        REPORT_LEGION_ERROR(ERROR_INDEX_TREE_MISMATCH,
                            "IndexPartition %d is not part of the same "
                            "index tree as IndexPartition %d in create "
                            "cross product partitions!",
                            handle1.id, handle2.id)
#endif
      LegionColor partition_color = INVALID_COLOR;
      if (color != LEGION_AUTO_GENERATE_ID)
        partition_color = color;
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      ApEvent term_event = part_op->get_completion_event();
      // We need an owner node to decide which color everyone is going to use
      if (owner_shard->shard_id == index_partition_allocator_shard)
      {
        // Do the call on the owner node
        std::set<RtEvent> safe_events;
        runtime->forest->create_pending_cross_product(this, handle1, handle2, 
                                           handles, kind, partition_color, 
                                           term_event, safe_events, 
                                           owner_shard->shard_id, total_shards);
        // We need to wait on the safe event here to make sure effects
        // have been broadcast before letting the other shard to their part
        if (!safe_events.empty())
        {
          const RtEvent wait_on = Runtime::merge_events(safe_events);
          if (wait_on.exists() && !wait_on.has_triggered())
            wait_on.wait();
        }
        // Now broadcast the chosen color to all the other shards
        ValueBroadcast<LegionColor> color_collective(this, COLLECTIVE_LOC_15);
        color_collective.broadcast(partition_color);
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        // Wait for the creation to be done
        creation_barrier.wait();
      }
      else
      {
        // Get the color result from the owner node
        ValueBroadcast<LegionColor> color_collective(this,
                          index_partition_allocator_shard, COLLECTIVE_LOC_15);
        partition_color = color_collective.get_value();
#ifdef DEBUG_LEGION
        assert(partition_color != INVALID_COLOR);
#endif
        // Now we can do the call from this node
        std::set<RtEvent> safe_events;
        runtime->forest->create_pending_cross_product(this, handle1, handle2, 
                                           handles, kind, partition_color, 
                                           term_event, safe_events, 
                                           owner_shard->shard_id, total_shards);
        // Signal that we're done with our creation
        RtEvent safe_event;
        if (!safe_events.empty())
          safe_event = Runtime::merge_events(safe_events);
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/, safe_event);
        // Also have to wait for creation to finish on all shards because
        // any shard can handle requests for any cross-product partition
        creation_barrier.wait();
      }
      advance_replicate_barrier(creation_barrier, total_shards);
      part_op->initialize_cross_product(this, handle1, handle2,partition_color);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // If we have any handles then we need to perform an exchange so
      // that all the shards have all the names for the handles they need
      if (!handles.empty())
      {
        CrossProductCollective collective(this, COLLECTIVE_LOC_36);
        collective.exchange_partitions(handles);
      }
      // Update our allocation shard
      index_partition_allocator_shard++;
      if (index_partition_allocator_shard == total_shards)
        index_partition_allocator_shard = 0;
      if (runtime->verify_partitions)
      {
        Domain color_space = runtime->get_index_partition_color_space(handle1);
        // This code will only work if the color space has type coord_t
        TypeTag type_tag;
        switch (color_space.get_dim())
        {
#define DIMFUNC(DIM) \
          case DIM: \
            { \
              type_tag = NT_TemplateHelper::encode_tag<DIM,coord_t>(); \
              assert(handle1.get_type_tag() == type_tag); \
              break; \
            }
          LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
          default:
            assert(false);
        }
        for (Domain::DomainPointIterator itr(color_space); itr; itr++)
        {
          IndexSpace subspace;
          switch (color_space.get_dim())
          {
#define DIMFUNC(DIM) \
            case DIM: \
              { \
                const Point<DIM,coord_t> p(itr.p); \
                subspace = runtime->get_index_subspace(handle1, &p, type_tag); \
                break; \
              }
            LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
            default:
              assert(false);
          }
          IndexPartition part = 
            runtime->get_index_partition(subspace, partition_color);
          verify_partition(part, verify_kind, __func__);
        }
      }
      return partition_color;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_association(LogicalRegion domain,
                                              LogicalRegion domain_parent,
                                              FieldID domain_fid,
                                              IndexSpace range,
                                              MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_ASSOCIATION);
        hasher.hash(domain);
        hasher.hash(domain_parent);
        hasher.hash(domain_fid);
        hasher.hash(range);
        hasher.hash(id);
        hasher.hash(tag);
        verify_replicable(hasher, "create_association");
      }
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_index.debug("Creating association in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_37));
#endif
      part_op->initialize_by_association(this, domain, domain_parent, 
          domain_fid, range, id, tag, dependent_partition_barrier);
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_association call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_restricted_partition(
                                              IndexSpace parent,
                                              IndexSpace color_space,
                                              const void *transform,
                                              size_t transform_size,
                                              const void *extent,
                                              size_t extent_size,
                                              PartitionKind part_kind,
                                              Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_RESTRICTED_PARTITION);
        hasher.hash(parent);
        hasher.hash(color_space);
        hasher.hash(transform, transform_size);
        hasher.hash(extent, extent_size);
        hasher.hash(part_kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_restricted_partition");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color; 
      else
        color_generated = true;
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_64);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, part_kind,
                        part_color, color_generated, disjoint_result))
        log_index.debug("Creating restricted partition in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_restricted_partition(this, pid, transform, 
                                transform_size, extent, extent_size);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Now update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_domain(
                                                IndexSpace parent,
                                    const std::map<DomainPoint,Domain> &domains,
                                                IndexSpace color_space,
                                                bool perform_intersections,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_DOMAIN);
        hasher.hash(parent);
        Serializer rez;
        for (std::map<DomainPoint,Domain>::const_iterator it = 
              domains.begin(); it != domains.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        hasher.hash(color_space);
        hasher.hash(perform_intersections);
        hasher.hash(part_kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_domain");
      }
      Domain fm_domain;
      RtUserEvent deletion_precondition;
      // Have to include all the points in the domain computation
      switch (color_space.get_dim())
      {
#define DIMFUNC(DIM) \
        case DIM: \
          { \
            std::vector<Realm::Point<DIM,coord_t> > points(domains.size());\
            unsigned index = 0; \
            for (std::map<DomainPoint,Domain>::const_iterator it = \
                  domains.begin(); it != domains.end(); it++) \
            { \
              const Point<DIM,coord_t> point = it->first; \
              points[index++] = point; \
            } \
            Realm::IndexSpace<DIM,coord_t> space(points); \
            const DomainT<DIM,coord_t> domaint(space); \
            fm_domain = domaint; \
            if (!space.dense()) \
            { \
              deletion_precondition = Runtime::create_rt_user_event(); \
              space.destroy(deletion_precondition); \
            } \
            break; \
          }
          LEGION_FOREACH_N(DIMFUNC)
#undef DIMFUNC
        default:
          assert(false);
      }
      const DistributedID did = runtime->get_available_distributed_id();
      FutureMap future_map(new FutureMapImpl(this, runtime, fm_domain, did,
            __sync_add_and_fetch(&outstanding_children_count, 1),
            runtime->address_space, RtEvent::NO_RT_EVENT, true/*reg now*/,
            deletion_precondition));
      // Prune out every N-th one for this shard and then pass through
      // the subset to the normal InnerContext variation of this
      ShardID shard = 0;
      std::map<DomainPoint,Future> shard_futures;
      for (std::map<DomainPoint,Domain>::const_iterator it = 
            domains.begin(); it != domains.end(); it++)
      {
        if (shard++ == owner_shard->shard_id)
          shard_futures[it->first] = Future::from_untyped_pointer(
              runtime->external, &it->second, sizeof(it->second));
        if (shard == total_shards)
          shard = 0;
      }
      future_map.impl->set_all_futures(shard_futures);
      return create_partition_by_domain(parent, future_map, color_space, 
                                        perform_intersections, part_kind,color);
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_domain(
                                                    IndexSpace parent,
                                                    const FutureMap &domains,
                                                    IndexSpace color_space,
                                                    bool perform_intersections,
                                                    PartitionKind part_kind,
                                                    Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_DOMAIN);
        hasher.hash(parent);
        hash_future_map(hasher, domains);
        hasher.hash(color_space);
        hasher.hash(perform_intersections);
        hasher.hash(part_kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_partition_by_domain");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color; 
      else
        color_generated = true;
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_76);
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, part_kind,
                        part_color, color_generated, disjoint_result))
        log_index.debug("Creating partition by domain in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_domain(this, pid, domains, perform_intersections);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      // Now update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_field(
                                              LogicalRegion handle,
                                              LogicalRegion parent_priv,
                                              FieldID fid,
                                              IndexSpace color_space,
                                              Color color,
                                              MapperID id, MappingTagID tag,
                                              PartitionKind part_kind)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_FIELD);
        hasher.hash(handle);
        hasher.hash(parent_priv);
        hasher.hash(fid);
        hasher.hash(color_space);
        hasher.hash(color);
        hasher.hash(id);
        hasher.hash(tag);
        hasher.hash(part_kind);
        verify_replicable(hasher, "create_partition_by_field");
      }
      // Partition by field is disjoint by construction
      PartitionKind verify_kind = LEGION_DISJOINT_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      IndexSpace parent = handle.get_index_space();  
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      else
        color_generated = true;
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, part_kind,
                                 part_color, color_generated))
        log_index.debug("Creating partition by field in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      // Allocate the partition operation
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_field(this, index_partition_allocator_shard,
                                   pending_partition_barrier, pid, handle, 
                                   parent_priv, fid, id, tag,
                                   dependent_partition_barrier);
#ifdef DEBUG_LEGION
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_38));
#endif
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_partition_by_field call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_image(
                                                    IndexSpace handle,
                                                    LogicalPartition projection,
                                                    LogicalRegion parent,
                                                    FieldID fid,
                                                    IndexSpace color_space,
                                                    PartitionKind part_kind,
                                                    Color color,
                                                    MapperID id, 
                                                    MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_IMAGE);
        hasher.hash(handle);
        hasher.hash(projection);
        hasher.hash(parent);
        hasher.hash(fid);
        hasher.hash(color_space);
        hasher.hash(part_kind);
        hasher.hash(color);
        hasher.hash(id);
        hasher.hash(tag);
        verify_replicable(hasher, "create_partition_by_image");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      else
        color_generated = true;
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_65);
      IndexPartition pid(0/*temp*/, handle.get_tree_id(),handle.get_type_tag());
      if (create_shard_partition(pid, handle, color_space, part_kind,
                        part_color, color_generated, disjoint_result))
        log_index.debug("Creating partition by image in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      // Allocate the partition operation
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_image(this, 
#ifndef SHARD_BY_IMAGE
                                   index_partition_allocator_shard,
#endif
                                   pending_partition_barrier, 
                                   pid, projection, parent, fid, id, tag,
                                   owner_shard->shard_id, total_shards,
                                   dependent_partition_barrier);
#ifdef DEBUG_LEGION
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_39));
#endif
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_partition_by_image call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_image_range(
                                                    IndexSpace handle,
                                                    LogicalPartition projection,
                                                    LogicalRegion parent,
                                                    FieldID fid,
                                                    IndexSpace color_space,
                                                    PartitionKind part_kind,
                                                    Color color,
                                                    MapperID id, 
                                                    MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_IMAGE_RANGE);
        hasher.hash(handle);
        hasher.hash(projection);
        hasher.hash(parent);
        hasher.hash(fid);
        hasher.hash(color_space);
        hasher.hash(part_kind);
        hasher.hash(color);
        hasher.hash(id);
        hasher.hash(tag);
        verify_replicable(hasher, "create_partition_by_image_range");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      else
        color_generated = true;
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_66);
      IndexPartition pid(0/*temp*/, handle.get_tree_id(),handle.get_type_tag());
      if (create_shard_partition(pid, handle, color_space, part_kind,
                        part_color, color_generated, disjoint_result))
        log_index.debug("Creating partition by image range in task %s "
                        "(ID %lld)", get_task_name(), get_unique_id());
      // Allocate the partition operation
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_image_range(this, 
#ifndef SHARD_BY_IMAGE
                                         index_partition_allocator_shard,
#endif
                                         pending_partition_barrier,
                                         pid, projection, parent, fid, id, tag,
                                         owner_shard->shard_id, total_shards,
                                         dependent_partition_barrier);
#ifdef DEBUG_LEGION
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_40));
#endif
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_partition_by_image_range call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_preimage(
                                                  IndexPartition projection,
                                                  LogicalRegion handle,
                                                  LogicalRegion parent,
                                                  FieldID fid,
                                                  IndexSpace color_space,
                                                  PartitionKind part_kind,
                                                  Color color,
                                                  MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_PREIMAGE);
        hasher.hash(projection);
        hasher.hash(handle);
        hasher.hash(parent);
        hasher.hash(fid);
        hasher.hash(color_space);
        hasher.hash(part_kind);
        hasher.hash(color);
        hasher.hash(id);
        hasher.hash(tag);
        verify_replicable(hasher, "create_partition_by_preimage");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      else
        color_generated = true; 
      // If the source of the preimage is disjoint then the result is disjoint
      // Note this only applies here and not to range
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        IndexPartNode *p = runtime->forest->get_node(projection);
        if (p->is_disjoint(true/*from app*/))
        {
          if (part_kind == LEGION_COMPUTE_KIND)
            part_kind = LEGION_DISJOINT_KIND;
          else if (part_kind == LEGION_COMPUTE_COMPLETE_KIND)
            part_kind = LEGION_DISJOINT_COMPLETE_KIND;
          else
            part_kind = LEGION_DISJOINT_INCOMPLETE_KIND;
        }
      }
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_67);
      IndexPartition pid(0/*temp*/,
          handle.get_index_space().get_tree_id(), parent.get_type_tag());
      if (create_shard_partition(pid, handle.get_index_space(), color_space, 
                    part_kind, part_color, color_generated, disjoint_result))
        log_index.debug("Creating partition by preimage in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
      // Allocate the partition operation
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_preimage(this, index_partition_allocator_shard,
                                      pending_partition_barrier,
                                      pid, projection, handle,
                                      parent, fid, id, tag, 
                                      dependent_partition_barrier);
#ifdef DEBUG_LEGION
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_41));
#endif
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_partition_by_preimage call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_partition_by_preimage_range(
                                                  IndexPartition projection,
                                                  LogicalRegion handle,
                                                  LogicalRegion parent,
                                                  FieldID fid,
                                                  IndexSpace color_space,
                                                  PartitionKind part_kind,
                                                  Color color,
                                                  MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);  
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PARTITION_BY_PREIMAGE_RANGE);
        hasher.hash(projection);
        hasher.hash(handle);
        hasher.hash(parent);
        hasher.hash(fid);
        hasher.hash(color_space);
        hasher.hash(part_kind);
        hasher.hash(color);
        hasher.hash(id);
        hasher.hash(tag);
        verify_replicable(hasher, "create_partition_by_preimage_range");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color;
      else
        color_generated = true; 
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_68);
      IndexPartition pid(0/*temp*/,
          handle.get_index_space().get_tree_id(), parent.get_type_tag());
      if (create_shard_partition(pid, handle.get_index_space(), color_space, 
                    part_kind, part_color, color_generated, disjoint_result))
        log_index.debug("Creating partition by preimage range in task %s "
                        "(ID %lld)", get_task_name(), get_unique_id());
      // Allocate the partition operation
      ReplDependentPartitionOp *part_op = 
        runtime->get_available_repl_dependent_partition_op();
      const ApEvent term_event = part_op->get_completion_event();
      part_op->initialize_by_preimage_range(this, 
                                            index_partition_allocator_shard, 
                                            pending_partition_barrier,
                                            pid, projection, handle,
                                            parent, fid, id, tag,
                                            dependent_partition_barrier);
#ifdef DEBUG_LEGION
      part_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_42));
#endif
      // Now figure out if we need to unmap and re-map any inline mappings
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(part_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around create_partition_by_preimage_range call "
              "in task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(part_op);
      // Update the pending partition barrier
      Runtime::phase_barrier_arrive(pending_partition_barrier,
                                    1/*count*/, term_event);
      advance_replicate_barrier(pending_partition_barrier, total_shards);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      if (runtime->verify_partitions)
        verify_partition(pid, verify_kind, __func__);
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexPartition ReplicateContext::create_pending_partition(
                                                      IndexSpace parent,
                                                      IndexSpace color_space,
                                                      PartitionKind part_kind,
                                                      Color color)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PENDING_PARTITION);
        hasher.hash(parent);
        hasher.hash(color_space);
        hasher.hash(part_kind);
        hasher.hash(color);
        verify_replicable(hasher, "create_pending_partition");
      }
      PartitionKind verify_kind = LEGION_COMPUTE_KIND;
      if (runtime->verify_partitions)
        SWAP_PART_KINDS(verify_kind, part_kind)
      LegionColor part_color = INVALID_COLOR;
      bool color_generated = false;
      if (color != LEGION_AUTO_GENERATE_ID)
        part_color = color; 
      else
        color_generated = true;
      ValueBroadcast<bool> *disjoint_result = NULL;
      if ((part_kind == LEGION_COMPUTE_KIND) || 
          (part_kind == LEGION_COMPUTE_COMPLETE_KIND) ||
          (part_kind == LEGION_COMPUTE_INCOMPLETE_KIND))
        disjoint_result = new ValueBroadcast<bool>(this, 
            pending_index_partitions.empty() ? index_partition_allocator_shard :
            pending_index_partitions.front().second, COLLECTIVE_LOC_69);
      ApBarrier partition_ready;
      if (owner_shard->shard_id == index_partition_allocator_shard)
      {
        // We have to make a barrier to be used for this partition
        size_t color_space_size = 
          runtime->forest->get_domain_volume(color_space);
        partition_ready = 
          ApBarrier(Realm::Barrier::create_barrier(color_space_size));
        ValueBroadcast<ApBarrier> bar_collective(this, COLLECTIVE_LOC_30);
        bar_collective.broadcast(partition_ready);
      }
      else
      {
        ValueBroadcast<ApBarrier> bar_collective(this,
                      index_partition_allocator_shard, COLLECTIVE_LOC_30);
        partition_ready = bar_collective.get_value();
      }
      // Update our allocation shard
      index_partition_allocator_shard++;
      if (index_partition_allocator_shard == total_shards)
        index_partition_allocator_shard = 0;
      IndexPartition pid(0/*temp*/,parent.get_tree_id(),parent.get_type_tag());
      if (create_shard_partition(pid, parent, color_space, part_kind,
            part_color, color_generated, disjoint_result, partition_ready))
        log_index.debug("Creating pending partition in task %s (ID %lld)", 
                        get_task_name(), get_unique_id());
      if (runtime->verify_partitions)
      {
        // We can't block to check this here because the user needs 
        // control back in order to fill in the pieces of the partitions
        // so just launch a meta-task to check it when we can
        VerifyPartitionArgs args(this, pid, verify_kind, __func__);
        runtime->issue_runtime_meta_task(args, LG_LOW_PRIORITY, 
            Runtime::protect_event(partition_ready));
      }
      return pid;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_union(
                                                    IndexPartition parent,
                                                    const void *realm_color,
                                                    size_t color_size,
                                                    TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE_UNION);
        hasher.hash(parent);
        hasher.hash(realm_color, color_size);
        hasher.hash(type_tag);
        for (std::vector<IndexSpace>::const_iterator it = 
              handles.begin(); it != handles.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "create_index_space_union");
      }
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space union in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag);
      part_op->initialize_index_space_union(this, result, handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_union(
                                                      IndexPartition parent,
                                                      const void *realm_color,
                                                      size_t color_size,
                                                      TypeTag type_tag,
                                                      IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE_UNION);
        hasher.hash(parent);
        hasher.hash(realm_color, color_size);
        hasher.hash(type_tag);
        hasher.hash(handle);
        verify_replicable(hasher, "create_index_space_union");
      }
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space union in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag);
      part_op->initialize_index_space_union(this, result, handle);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_intersection(
                                                    IndexPartition parent,
                                                    const void *realm_color,
                                                    size_t color_size,
                                                    TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE_INTERSECTION);
        hasher.hash(parent);
        hasher.hash(realm_color, color_size);
        hasher.hash(type_tag);
        for (std::vector<IndexSpace>::const_iterator it = 
              handles.begin(); it != handles.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "create_index_space_intersection");
      }
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space intersection in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_intersection(this, result, handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_intersection(
                                                    IndexPartition parent,
                                                    const void *realm_color,
                                                    size_t color_size,
                                                    TypeTag type_tag,
                                                    IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE_INTERSECTION);
        hasher.hash(parent);
        hasher.hash(realm_color, color_size);
        hasher.hash(type_tag);
        hasher.hash(handle);
        verify_replicable(hasher, "create_index_space_intersection");
      }
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space intersection in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_intersection(this, result, handle);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    IndexSpace ReplicateContext::create_index_space_difference(
                                                    IndexPartition parent,
                                                    const void *realm_color,
                                                    size_t color_size,
                                                    TypeTag type_tag,
                                                    IndexSpace initial,
                                        const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_INDEX_SPACE_DIFFERENCE);
        hasher.hash(parent);
        hasher.hash(realm_color, color_size);
        hasher.hash(type_tag);
        hasher.hash(initial);
        for (std::vector<IndexSpace>::const_iterator it = 
              handles.begin(); it != handles.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "create_index_space_difference");
      }
#ifdef DEBUG_LEGION
      log_index.debug("Creating index space difference in task %s (ID %lld)", 
                      get_task_name(), get_unique_id());
#endif
      ReplPendingPartitionOp *part_op = 
        runtime->get_available_repl_pending_partition_op();
      IndexSpace result = 
        runtime->forest->get_index_subspace(parent, realm_color, type_tag); 
      part_op->initialize_index_space_difference(this, result, initial,handles);
      // Now we can add the operation to the queue
      add_to_dependence_queue(part_op);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::verify_partition(IndexPartition pid, 
                                  PartitionKind kind, const char *function_name)
    //--------------------------------------------------------------------------
    {
      IndexPartNode *node = runtime->forest->get_node(pid);
      // Check containment first
      if (node->total_children == node->max_linearized_color)
      {
        for (LegionColor color = owner_shard->shard_id; 
              color < node->total_children; color+=total_shards)
        {
          IndexSpaceNode *child_node = node->get_child(color);
          IndexSpaceExpression *diff = 
            runtime->forest->subtract_index_spaces(child_node, node->parent);
          if (!diff->is_empty())
          {
            const DomainPoint bad = 
              node->color_space->delinearize_color_to_point(color);
            switch (bad.get_dim())
            {
              case 1:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0])
              case 2:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1])
              case 3:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2])
              case 4:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3])
              case 5:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4])
              case 6:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5])
              case 7:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6])
              case 8:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7])
              case 9:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7], bad[8])
              default:
                assert(false);
            }
          }
        }
      }
      else
      {
        ColorSpaceIterator *itr =
          node->color_space->create_color_space_iterator();
        // Skip ahead if necessary for our shard
        for (unsigned idx = 0; idx < owner_shard->shard_id; idx++)
        {
          itr->yield_color();
          if (!itr->is_valid())
            break;
        }
        while (itr->is_valid())
        {
          const LegionColor color = itr->yield_color();
          IndexSpaceNode *child_node = node->get_child(color);
          IndexSpaceExpression *diff = 
            runtime->forest->subtract_index_spaces(child_node, node->parent);
          if (!diff->is_empty())
          {
            const DomainPoint bad = 
              node->color_space->delinearize_color_to_point(color);
            switch (bad.get_dim())
            {
              case 1:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0])
              case 2:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1])
              case 3:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2])
              case 4:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3])
              case 5:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4])
              case 6:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5])
              case 7:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6])
              case 8:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7])
              case 9:
                REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
                    "Call to partition function %s in %s (UID %lld) has "
                    "non-dominated child sub-region at color (%lld,%lld,"
                    "%lld,%lld,%lld,%lld,%lld,%lld,%lld).",
                    function_name, get_task_name(), get_unique_id(),
                    bad[0], bad[1], bad[2], bad[3], bad[4], bad[5], bad[6],
                    bad[7], bad[8])
              default:
                assert(false);
            }
            // Skip ahead for the next color if necessary
            for (unsigned idx = 0; idx < (total_shards-1); idx++)
            {
              itr->yield_color();
              if (!itr->is_valid())
                break;
            }
          }
        }
        delete itr;
      }
      // Only need to do the rest of this on shard 0
      if (owner_shard->shard_id > 0)
        return;
      // Check disjointness
      if ((kind == LEGION_DISJOINT_KIND) || 
          (kind == LEGION_DISJOINT_COMPLETE_KIND) ||
          (kind == LEGION_DISJOINT_INCOMPLETE_KIND))
      {
        if (!node->is_disjoint(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is aliased.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_KIND) ? "DISJOINT_KIND" :
              (kind == LEGION_DISJOINT_COMPLETE_KIND) ? 
              "DISJOINT_COMPLETE_KIND" : "DISJOINT_INCOMPLETE_KIND")
      }
      else if ((kind == LEGION_ALIASED_KIND) || 
               (kind == LEGION_ALIASED_COMPLETE_KIND) ||
               (kind == LEGION_ALIASED_INCOMPLETE_KIND))
      {
        if (node->is_disjoint(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is disjoint.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_ALIASED_KIND) ? "ALIASED_KIND" :
              (kind == LEGION_ALIASED_COMPLETE_KIND) ? "ALIASED_COMPLETE_KIND" :
              "ALIASED_INCOMPLETE_KIND")
      }
      // Check completeness
      if ((kind == LEGION_DISJOINT_COMPLETE_KIND) || 
          (kind == LEGION_ALIASED_COMPLETE_KIND) ||
          (kind == LEGION_COMPUTE_COMPLETE_KIND))
      {
        if (!node->is_complete(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is incomplete.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_COMPLETE_KIND) ? "DISJOINT_COMPLETE_KIND"
            : (kind == LEGION_ALIASED_COMPLETE_KIND) ? "ALIASED_COMPLETE_KIND" :
              "COMPUTE_COMPLETE_KIND")
      }
      else if ((kind == LEGION_DISJOINT_INCOMPLETE_KIND) || 
               (kind == LEGION_ALIASED_INCOMPLETE_KIND) || 
               (kind == LEGION_COMPUTE_INCOMPLETE_KIND))
      {
        if (node->is_complete(true/*from application*/))
          REPORT_LEGION_ERROR(ERROR_PARTITION_VERIFICATION,
              "Call to partitioning function %s in %s (UID %lld) specified "
              "partition was %s but the partition is complete.",
              function_name, get_task_name(), get_unique_id(),
              (kind == LEGION_DISJOINT_INCOMPLETE_KIND) ? 
                "DISJOINT_INCOMPLETE_KIND" :
              (kind == LEGION_ALIASED_INCOMPLETE_KIND) ? 
              "ALIASED_INCOMPLETE_KIND" : "COMPUTE_INCOMPLETE_KIND")
      }
    }

    //--------------------------------------------------------------------------
    FieldSpace ReplicateContext::create_field_space(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_FIELD_SPACE);
        verify_replicable(hasher, "create_field_space");
      }
      return create_replicated_field_space(); 
    }

    //--------------------------------------------------------------------------
    FieldSpace ReplicateContext::create_replicated_field_space(ShardID *creator)
    //--------------------------------------------------------------------------
    {
      // Seed this with the first field space broadcast
      if (pending_field_spaces.empty())
        increase_pending_field_spaces(1/*count*/, false/*double*/);
      FieldSpace space;
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<FSBroadcast>*,bool> &collective = 
        pending_field_spaces.front();
      if (creator != NULL)
        *creator = collective.first->origin;
      ShardMapping &shard_mapping = shard_manager->get_mapping();
      if (collective.second)
      {
        const FSBroadcast value = collective.first->get_value(false);
        space = FieldSpace(value.space_id);
        double_buffer = value.double_buffer;
        // Need to register this before broadcasting
        std::set<RtEvent> applied;
        FieldSpaceNode *node = runtime->forest->create_field_space(space, 
            value.did, false/*notify remote*/, creation_barrier, &applied,
            &shard_mapping);
        // Now we can update the creation set
        node->update_creation_set(shard_mapping);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/); 
        runtime->forest->revoke_pending_field_space(value.space_id);
        runtime->revoke_pending_distributed_collectable(value.did);
#ifdef DEBUG_LEGION
        log_field.debug("Creating field space %x in task %s (ID %lld)", 
                        space.id, get_task_name(), get_unique_id());
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_field_space(space.id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const FSBroadcast value = collective.first->get_value(false);
        space = FieldSpace(value.space_id);
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(space.exists());
#endif
        std::set<RtEvent> applied;
        runtime->forest->create_field_space(space, value.did,
            false/*notify remote*/, creation_barrier, &applied, &shard_mapping);
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_field_spaces.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Record this in our context
      register_field_space_creation(space);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_field_spaces(double_buffer ? 
          pending_field_spaces.size() + 1 : 1, double_next && !double_buffer);
      return space;
    }

    //--------------------------------------------------------------------------
    FieldSpace ReplicateContext::create_field_space(
                                         const std::vector<size_t> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_FIELD_SPACE);
        for (std::vector<size_t>::const_iterator it = 
              sizes.begin(); it != sizes.end(); it++)
          hasher.hash(*it);
        for (std::vector<FieldID>::const_iterator it = 
              resulting_fields.begin(); it != resulting_fields.end(); it++)
          hasher.hash(*it);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "create_field_space");
      }
      ShardID creator_shard = 0;
      const FieldSpace space = create_replicated_field_space(&creator_shard);
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
        {
          if (pending_fields.empty())
            increase_pending_fields(1/*count*/, false/*double*/);
          bool double_next = false;
          bool double_buffer = false;
          std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
            pending_fields.front();
          if (collective.second)
          {
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          else
          {
            const RtEvent done = collective.first->get_done_event();
            if (!done.has_triggered())
            {
              double_next = true;
              done.wait();
            }
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          delete collective.first;
          pending_fields.pop_front();
          increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                  double_next && !double_buffer);
        }
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
      }
      // Figure out if we're going to do the field initialization on this node
      const AddressSpaceID owner_space =
        FieldSpaceNode::get_owner_space(space, runtime);
      const bool local_shard = (owner_space == runtime->address_space) ?
        (creator_shard == owner_shard->shard_id) :
        shard_manager->is_first_local_shard(owner_shard);
      // This deduplicates multiple shards on the same node
      if (local_shard)
      {
        const bool non_owner = (creator_shard != owner_shard->shard_id);
        FieldSpaceNode *node = runtime->forest->get_node(space);
        node->initialize_fields(sizes, resulting_fields, serdez_id, true);
        if (runtime->legion_spy_enabled && !non_owner)
          for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
            LegionSpy::log_field_creation(space.id, 
                                          resulting_fields[idx], sizes[idx]);
      }
      Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      CreationOp *creator_op = runtime->get_available_creation_op();
      creator_op->initialize_fence(this, creation_barrier);
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_all_field_creations(space, false/*loca*/, resulting_fields);
      return space;
    }

    //--------------------------------------------------------------------------
    FieldSpace ReplicateContext::create_field_space(
                                         const std::vector<Future> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_FIELD_SPACE);
        for (std::vector<Future>::const_iterator it = 
              sizes.begin(); it != sizes.end(); it++)
          hash_future(hasher, runtime->safe_control_replication, *it);
        for (std::vector<FieldID>::const_iterator it = 
              resulting_fields.begin(); it != resulting_fields.end(); it++)
          hasher.hash(*it);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "create_field_space");
      }
      ShardID creator_shard = 0;
      const FieldSpace space = create_replicated_field_space(&creator_shard);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
        {
          if (pending_fields.empty())
            increase_pending_fields(1/*count*/, false/*double*/);
          bool double_next = false;
          bool double_buffer = false;
          std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
            pending_fields.front();
          if (collective.second)
          {
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          else
          {
            const RtEvent done = collective.first->get_done_event();
            if (!done.has_triggered())
            {
              double_next = true;
              done.wait();
            }
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          delete collective.first;
          pending_fields.pop_front();
          increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                  double_next && !double_buffer);
        }
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
      }
      for (unsigned idx = 0; idx < sizes.size(); idx++)
        if (sizes[idx].impl == NULL)
          REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
              "Invalid empty future passed to field allocation for field %d "
              "in task %s (UID %lld)", resulting_fields[idx],
              get_task_name(), get_unique_id())
      // Figure out if we're going to do the field initialization on this node
      const AddressSpaceID owner_space =
        FieldSpaceNode::get_owner_space(space, runtime);
      const bool local_shard = (owner_space == runtime->address_space) ?
        (creator_shard == owner_shard->shard_id) :
        shard_manager->is_first_local_shard(owner_shard);
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();
      // This deduplicates multiple shards on the same node
      if (local_shard)
      {
        const ApEvent ready = creator_op->get_completion_event();
        const bool owner = (creator_shard == owner_shard->shard_id);
        FieldSpaceNode *node = runtime->forest->get_node(space);
        node->initialize_fields(ready, resulting_fields, serdez_id, true);
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        creator_op->initialize_fields(this, node, resulting_fields, 
                                      sizes, RtEvent::NO_RT_EVENT, owner);
      }
      else
      {
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        creator_op->initialize_fence(this, creation_barrier);
      }
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_all_field_creations(space, false/*local*/, resulting_fields);
      return space;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::increase_pending_field_spaces(unsigned count,
                                                         bool double_next)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < count; idx++)
      {
        if (owner_shard->shard_id == field_space_allocator_shard)
        {
          const FieldSpaceID space = runtime->get_unique_field_space_id();
          const DistributedID did = runtime->get_available_distributed_id();
          // We're the owner, so make it locally and then broadcast it
          runtime->forest->record_pending_field_space(space);
          runtime->record_pending_distributed_collectable(did);
          // Do our arrival on this generation, should be the last one
          ValueBroadcast<FSBroadcast> *collective = 
            new ValueBroadcast<FSBroadcast>(this, COLLECTIVE_LOC_31);
          collective->broadcast(FSBroadcast(space, did, double_next));
          pending_field_spaces.push_back(
              std::pair<ValueBroadcast<FSBroadcast>*,bool>(collective, true));
        }
        else
        {
          ValueBroadcast<FSBroadcast> *collective = 
            new ValueBroadcast<FSBroadcast>(this, field_space_allocator_shard,
                                            COLLECTIVE_LOC_31);
          register_collective(collective);
          pending_field_spaces.push_back(
              std::pair<ValueBroadcast<FSBroadcast>*,bool>(collective, false));
        }
        field_space_allocator_shard++;
        if (field_space_allocator_shard == total_shards)
          field_space_allocator_shard = 0;
        double_next = false;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_shared_ownership(FieldSpace handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      if (shard_manager->is_total_sharding() &&
          shard_manager->is_first_local_shard(owner_shard))
        runtime->create_shared_ownership(handle, true/*total sharding*/);
      else if (owner_shard->shard_id == 0)
        runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<FieldSpace,unsigned>::iterator finder = 
        created_field_spaces.find(handle);
      if (finder != created_field_spaces.end())
        finder->second++;
      else
        created_field_spaces[handle] = 1;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_field_space(FieldSpace handle,
                                               const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_FIELD_SPACE);
        hasher.hash(handle);
        verify_replicable(hasher, "destroy_field_space");
      }
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_field.debug("Destroying field space %x in task %s (ID %lld)", 
                        handle.id, get_task_name(), get_unique_id());
#endif
      // Check to see if this is one that we should be allowed to destory
      {
        AutoLock priv_lock(privilege_lock);
        std::map<FieldSpace,unsigned>::iterator finder = 
          created_field_spaces.find(handle);
        if (finder != created_field_spaces.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_field_spaces.erase(finder);
          else
            return;
          // Count how many regions are still using this field space
          // that still need to be deleted before we can remove the
          // list of created fields
          std::set<LogicalRegion> latent_regions;
          for (std::map<LogicalRegion,unsigned>::const_iterator it = 
                created_regions.begin(); it != created_regions.end(); it++)
            if (it->first.get_field_space() == handle)
              latent_regions.insert(it->first);
          for (std::map<LogicalRegion,bool>::const_iterator it = 
                local_regions.begin(); it != local_regions.end(); it++)
            if (it->first.get_field_space() == handle)
              latent_regions.insert(it->first);
          if (latent_regions.empty())
          {
            // No remaining regions so we can remove any created fields now
            for (std::set<std::pair<FieldSpace,FieldID> >::iterator it = 
                  created_fields.begin(); it != 
                  created_fields.end(); /*nothing*/)
            {
              if (it->first == handle)
              {
                std::set<std::pair<FieldSpace,FieldID> >::iterator 
                  to_delete = it++;
                created_fields.erase(to_delete);
              }
              else
                it++;
            }
          }
          else
            latent_field_spaces[handle] = latent_regions;
        }
        else
        {
          // If we didn't make this field space, record the deletion
          // and keep going. It will be handled by the context that
          // made the field space
          deleted_field_spaces.push_back(handle);
          return;
        }
      }
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_field_space_deletion(this, handle, unordered);
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    FieldID ReplicateContext::allocate_field(FieldSpace space,size_t field_size,
                                             FieldID fid, bool local,
                                             CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ALLOCATE_FIELD);
        hasher.hash(space);
        hasher.hash(field_size);
        hasher.hash(fid);
        hasher.hash(local);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "allocate_field");
      }
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
                      "Local field creation is not currently supported "
                      "for control replication with task %s (UID %lld)",
                      get_task_name(), get_unique_id())
      if (fid == LEGION_AUTO_GENERATE_ID)
      {
        if (pending_fields.empty())
          increase_pending_fields(1/*count*/, false/*double*/);
        bool double_next = false;
        bool double_buffer = false;
        std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
          pending_fields.front();
        if (collective.second)
        {
          const FIDBroadcast value = collective.first->get_value(false);
          fid = value.field_id;
          double_buffer = value.double_buffer;
        }
        else
        {
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
          {
            double_next = true;
            done.wait();
          }
          const FIDBroadcast value = collective.first->get_value(false);
          fid = value.field_id;
          double_buffer = value.double_buffer;
        }
        delete collective.first;
        pending_fields.pop_front();
        increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                double_next && !double_buffer);
      }
      else if (fid >= LEGION_MAX_APPLICATION_FIELD_ID)
        REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
                     "Task %s (ID %lld) attempted to allocate a field with "
                     "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID"
                     " bound set in legion_config.h", get_task_name(),
                     get_unique_id(), fid)
      std::map<FieldSpace,std::pair<ShardID,bool> >::const_iterator finder = 
        field_allocator_owner_shards.find(space);
#ifdef DEBUG_LEGION
      assert(finder != field_allocator_owner_shards.end());
#endif
      RtEvent precondition;
      // This deduplicates multiple shards on the same node
      if (finder->second.second)
      {
        const bool non_owner = (finder->second.first != owner_shard->shard_id);
        precondition = runtime->forest->allocate_field(space, field_size, fid, 
                                                       serdez_id, non_owner);
        if (runtime->legion_spy_enabled && !non_owner)
          LegionSpy::log_field_creation(space.id, fid, field_size);
      }
      Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/, precondition);
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      CreationOp *creator_op = runtime->get_available_creation_op();
      creator_op->initialize_fence(this, creation_barrier);
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_field_creation(space, fid, local);
      return fid;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::increase_pending_fields(unsigned count, 
                                                   bool double_next)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < count; idx++)
      {
        if (owner_shard->shard_id == field_allocator_shard)
        {
          const FieldID fid = runtime->get_unique_field_id();
          // Do our arrival on this generation, should be the last one
          ValueBroadcast<FIDBroadcast> *collective = 
            new ValueBroadcast<FIDBroadcast>(this, COLLECTIVE_LOC_33);
          collective->broadcast(FIDBroadcast(fid, double_next));
          pending_fields.push_back(
              std::pair<ValueBroadcast<FIDBroadcast>*,bool>(collective, true));
        }
        else
        {
          ValueBroadcast<FIDBroadcast> *collective = 
            new ValueBroadcast<FIDBroadcast>(this, field_allocator_shard,
                                             COLLECTIVE_LOC_33);
          register_collective(collective);
          pending_fields.push_back(
              std::pair<ValueBroadcast<FIDBroadcast>*,bool>(collective, false));
        }
        field_allocator_shard++;
        if (field_allocator_shard == total_shards)
          field_allocator_shard = 0;
        double_next = false;
      }
    }

    //--------------------------------------------------------------------------
    FieldID ReplicateContext::allocate_field(FieldSpace space,
                                             const Future &field_size,
                                             FieldID fid, bool local,
                                             CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ALLOCATE_FIELD);
        hasher.hash(space);
        hash_future(hasher, runtime->safe_control_replication, field_size);
        hasher.hash(fid);
        hasher.hash(local);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "allocate_field");
      }
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
                      "Local field creation is not currently supported "
                      "for control replication with task %s (UID %lld)",
                      get_task_name(), get_unique_id())
      if (fid == LEGION_AUTO_GENERATE_ID)
      {
        if (pending_fields.empty())
          increase_pending_fields(1/*count*/, false/*double*/);
        bool double_next = false;
        bool double_buffer = false;
        std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
          pending_fields.front();
        if (collective.second)
        {
          const FIDBroadcast value = collective.first->get_value(false);
          fid = value.field_id;
          double_buffer = value.double_buffer;
        }
        else
        {
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
          {
            double_next = true;
            done.wait();
          }
          const FIDBroadcast value = collective.first->get_value(false);
          fid = value.field_id;
          double_buffer = value.double_buffer;
        }
        delete collective.first;
        pending_fields.pop_front();
        increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                double_next && !double_buffer);
      }
      else if (fid >= LEGION_MAX_APPLICATION_FIELD_ID)
        REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
                     "Task %s (ID %lld) attempted to allocate a field with "
                     "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID"
                     " bound set in legion_config.h", get_task_name(),
                     get_unique_id(), fid)
      if (field_size.impl == NULL)
        REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
            "Invalid empty future passed to field allocation for field %d "
            "in task %s (UID %lld)", fid, get_task_name(), get_unique_id())
      std::map<FieldSpace,std::pair<ShardID,bool> >::const_iterator finder = 
        field_allocator_owner_shards.find(space);
#ifdef DEBUG_LEGION
      assert(finder != field_allocator_owner_shards.end());
#endif
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();
      // This deduplicates multiple shards on the same node
      if (finder->second.second)
      {
        const ApEvent ready = creator_op->get_completion_event();
        const bool owner = (finder->second.first == owner_shard->shard_id);
        RtEvent precondition;
        FieldSpaceNode *node = runtime->forest->allocate_field(space, ready,
            fid, serdez_id, precondition, !owner);
        Runtime::phase_barrier_arrive(creation_barrier,1/*count*/,precondition);
        creator_op->initialize_field(this, node, fid, field_size, 
                                     precondition, owner);
      }
      else
      {
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        creator_op->initialize_fence(this, creation_barrier);
      }
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_field_creation(space, fid, local);
      return fid;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::free_field(FieldAllocatorImpl *allocator, 
                            FieldSpace space, FieldID fid, const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_FREE_FIELD);
        hasher.hash(space);
        hasher.hash(fid);
        verify_replicable(hasher, "free_field");
      }
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        const std::pair<FieldSpace,FieldID> key(space, fid);
        // This field will actually be removed in analyze_destroy_fields
        std::set<std::pair<FieldSpace,FieldID> >::const_iterator finder = 
          created_fields.find(key);
        if (finder == created_fields.end())
        {
          std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
            local_finder = local_fields.find(key);
          if (local_finder == local_fields.end())
          {
            // If we didn't make this field, record the deletion and
            // then have a later context handle it
            deleted_fields.push_back(key);
            return;
          }
          else
            local_finder->second = true;
        }
      }
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_field_deletion(this, space, fid, unordered, allocator,
                                    (owner_shard->shard_id != 0));
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::allocate_fields(FieldSpace space,
                                         const std::vector<size_t> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         bool local, CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ALLOCATE_FIELDS);
        hasher.hash(space);
        for (std::vector<size_t>::const_iterator it = 
              sizes.begin(); it != sizes.end(); it++)
          hasher.hash(*it);
        for (std::vector<FieldID>::const_iterator it = 
              resulting_fields.begin(); it != resulting_fields.end(); it++)
          hasher.hash(*it);
        hasher.hash(local);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "allocate_fields");
      }
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
                      "Local field creation is not currently supported "
                      "for control replication with task %s (UID %lld)",
                      get_task_name(), get_unique_id())
      if (resulting_fields.size() < sizes.size())
        resulting_fields.resize(sizes.size(), LEGION_AUTO_GENERATE_ID);
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
        {
          if (pending_fields.empty())
            increase_pending_fields(1/*count*/, false/*double*/);
          bool double_next = false;
          bool double_buffer = false;
          std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
            pending_fields.front();
          if (collective.second)
          {
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          else
          {
            const RtEvent done = collective.first->get_done_event();
            if (!done.has_triggered())
            {
              double_next = true;
              done.wait();
            }
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          delete collective.first;
          pending_fields.pop_front();
          increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                  double_next && !double_buffer);
        }
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
      }
      std::map<FieldSpace,std::pair<ShardID,bool> >::const_iterator finder = 
        field_allocator_owner_shards.find(space);
#ifdef DEBUG_LEGION
      assert(finder != field_allocator_owner_shards.end());
#endif
      RtEvent precondition;
      // This deduplicates multiple shards on the same node
      if (finder->second.second)
      {
        const bool non_owner = (finder->second.first != owner_shard->shard_id);
        precondition = runtime->forest->allocate_fields(space, sizes, 
                              resulting_fields, serdez_id, non_owner);
        if (runtime->legion_spy_enabled && !non_owner)
          for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
            LegionSpy::log_field_creation(space.id, 
                                          resulting_fields[idx], sizes[idx]);
      }
      Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/, precondition);
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      CreationOp *creator_op = runtime->get_available_creation_op();
      creator_op->initialize_fence(this, creation_barrier);
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_all_field_creations(space, local, resulting_fields);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::allocate_fields(FieldSpace space,
                                         const std::vector<Future> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         bool local, CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ALLOCATE_FIELDS);
        hasher.hash(space);
        for (std::vector<Future>::const_iterator it = 
              sizes.begin(); it != sizes.end(); it++)
          hash_future(hasher, runtime->safe_control_replication, *it);
        for (std::vector<FieldID>::const_iterator it = 
              resulting_fields.begin(); it != resulting_fields.end(); it++)
          hasher.hash(*it);
        hasher.hash(local);
        hasher.hash(serdez_id);
        verify_replicable(hasher, "allocate_fields");
      }
      if (local)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
                      "Local field creation is not currently supported "
                      "for control replication with task %s (UID %lld)",
                      get_task_name(), get_unique_id())
      for (unsigned idx = 0; idx < resulting_fields.size(); idx++)
      {
        if (resulting_fields[idx] == LEGION_AUTO_GENERATE_ID)
        {
          if (pending_fields.empty())
            increase_pending_fields(1/*count*/, false/*double*/);
          bool double_next = false;
          bool double_buffer = false;
          std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
            pending_fields.front();
          if (collective.second)
          {
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          else
          {
            const RtEvent done = collective.first->get_done_event();
            if (!done.has_triggered())
            {
              double_next = true;
              done.wait();
            }
            const FIDBroadcast value = collective.first->get_value(false);
            resulting_fields[idx] = value.field_id;
            double_buffer = value.double_buffer;
          }
          delete collective.first;
          pending_fields.pop_front();
          increase_pending_fields(double_buffer ? pending_fields.size() + 1 : 1,
                                  double_next && !double_buffer);
        }
#ifdef DEBUG_LEGION
        else if (resulting_fields[idx] >= LEGION_MAX_APPLICATION_FIELD_ID)
          REPORT_LEGION_ERROR(ERROR_TASK_ATTEMPTED_ALLOCATE_FIELD,
            "Task %s (ID %lld) attempted to allocate a field with "
            "ID %d which exceeds the LEGION_MAX_APPLICATION_FIELD_ID "
            "bound set in legion_config.h", get_task_name(),
            get_unique_id(), resulting_fields[idx])
#endif
      }
      for (unsigned idx = 0; idx < sizes.size(); idx++)
        if (sizes[idx].impl == NULL)
          REPORT_LEGION_ERROR(ERROR_REQUEST_FOR_EMPTY_FUTURE,
              "Invalid empty future passed to field allocation for field %d "
              "in task %s (UID %lld)", resulting_fields[idx],
              get_task_name(), get_unique_id())
      std::map<FieldSpace,std::pair<ShardID,bool> >::const_iterator finder = 
        field_allocator_owner_shards.find(space);
#ifdef DEBUG_LEGION
      assert(finder != field_allocator_owner_shards.end());
#endif
      // Get a new creation operation
      CreationOp *creator_op = runtime->get_available_creation_op();
      // This deduplicates multiple shards on the same node
      if (finder->second.second)
      {
        const ApEvent ready = creator_op->get_completion_event();
        const bool owner = (finder->second.first == owner_shard->shard_id);
        RtEvent precondition;
        FieldSpaceNode *node = runtime->forest->allocate_fields(space, ready,
                          resulting_fields, serdez_id, precondition, !owner);
        Runtime::phase_barrier_arrive(creation_barrier,1/*count*/,precondition);
        creator_op->initialize_fields(this, node, resulting_fields, 
                                      sizes, precondition, owner);
      }
      else
      {
        Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        creator_op->initialize_fence(this, creation_barrier);
      }
      // Launch the creation op in this context to act as a fence to ensure
      // that the allocations are done on all shard nodes before anyone else
      // tries to use them or their meta-data
      add_to_dependence_queue(creator_op);
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      register_all_field_creations(space, local, resulting_fields);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::free_fields(FieldAllocatorImpl *allocator,
                                       FieldSpace space, 
                                       const std::set<FieldID> &to_free,
                                       const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_FREE_FIELDS);
        hasher.hash(space);
        for (std::set<FieldID>::const_iterator it = 
              to_free.begin(); it != to_free.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "free_fields");
      }
      std::set<FieldID> free_now;
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        // These fields will actually be removed in analyze_destroy_fields
        for (std::set<FieldID>::const_iterator it = 
              to_free.begin(); it != to_free.end(); it++)
        {
          const std::pair<FieldSpace,FieldID> key(space, *it);
          std::set<std::pair<FieldSpace,FieldID> >::const_iterator finder = 
            created_fields.find(key);
          if (finder == created_fields.end())
          {
            std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
              local_finder = local_fields.find(key);
            if (local_finder != local_fields.end())
            {
              local_finder->second = true;
              free_now.insert(*it);
            }
            else
              deleted_fields.push_back(key);
          }
          else
            free_now.insert(*it);
        }
      }
      if (free_now.empty())
        return;
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_field_deletions(this, space, free_now, unordered, 
                                     allocator, (owner_shard->shard_id != 0));
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    LogicalRegion ReplicateContext::create_logical_region(
                                                      RegionTreeForest *forest,
                                                      IndexSpace index_space,
                                                      FieldSpace field_space,
                                                      const bool task_local,
                                                      const bool output_region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_LOGICAL_REGION);
        hasher.hash(index_space);
        hasher.hash(field_space);
        hasher.hash(task_local);
        verify_replicable(hasher, "create_logical_region");
      }
      // Seed this with the first field space broadcast
      if (pending_region_trees.empty())
        increase_pending_region_trees(1/*count*/, false/*double*/);
      LogicalRegion handle(0/*temp*/, index_space, field_space);
      bool double_next = false;
      bool double_buffer = false;
      std::pair<ValueBroadcast<LRBroadcast>*,bool> &collective = 
        pending_region_trees.front();
      if (collective.second)
      {
        const LRBroadcast value = collective.first->get_value(false);
        handle.tree_id = value.tid;
        double_buffer = value.double_buffer;
        std::set<RtEvent> applied;
        // Have to register this before doing the broadcast
        RegionNode *node = 
          forest->create_logical_region(handle, value.did,
              false/*notify remote*/, creation_barrier, &applied);
        // Now we can update the creation set
        node->update_creation_set(shard_manager->get_mapping());
        // Arrive on the creation barrier
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
        runtime->forest->revoke_pending_region_tree(value.tid);
#ifdef DEBUG_LEGION
        log_region.debug("Creating logical region in task %s (ID %lld) with "
                         "index space %x and field space %x in new tree %d",
                         get_task_name(), get_unique_id(), index_space.id, 
                         field_space.id, handle.tree_id);
#endif
        if (runtime->legion_spy_enabled)
          LegionSpy::log_top_region(index_space.id, field_space.id, 
                                    handle.tree_id);
      }
      else
      {
        const RtEvent done = collective.first->get_done_event();
        if (!done.has_triggered())
        {
          double_next = true;
          done.wait();
        }
        const LRBroadcast value = collective.first->get_value(false);
        handle.tree_id = value.tid;
        double_buffer = value.double_buffer;
#ifdef DEBUG_LEGION
        assert(handle.exists());
#endif
        std::set<RtEvent> applied;
        forest->create_logical_region(handle, value.did,
            false/*notify remote*/, creation_barrier, &applied);
        // Signal that we are done our creation
        if (!applied.empty())
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/,
              Runtime::merge_events(applied));
        else
          Runtime::phase_barrier_arrive(creation_barrier, 1/*count*/);
      }
      delete collective.first;
      pending_region_trees.pop_front();
      // Advance the creation barrier so that we know when it is ready
      advance_replicate_barrier(creation_barrier, total_shards);
      // Register the creation of a top-level region with the context
      register_region_creation(handle, task_local, output_region);
      // Get new handles in flight for the next time we need them
      // Always add a new one to replace the old one, but double the number
      // in flight if we're not hiding the latency
      increase_pending_region_trees(double_buffer ? 
          pending_region_trees.size() + 1 : 1, double_next && !double_buffer);
      return handle;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::increase_pending_region_trees(unsigned count,
                                                         bool double_next)
    //--------------------------------------------------------------------------
    {
      for (unsigned idx = 0; idx < count; idx++)
      {
        if (owner_shard->shard_id == logical_region_allocator_shard)
        {
          const RegionTreeID tid = runtime->get_unique_region_tree_id();
          const DistributedID did = runtime->get_available_distributed_id();
          // We're the owner, so make it locally and then broadcast it
          runtime->forest->record_pending_region_tree(tid);
          // Do our arrival on this generation, should be the last one
          ValueBroadcast<LRBroadcast> *collective = 
            new ValueBroadcast<LRBroadcast>(this, COLLECTIVE_LOC_34);
          collective->broadcast(LRBroadcast(tid, did, double_next));
          pending_region_trees.push_back(
              std::pair<ValueBroadcast<LRBroadcast>*,bool>(collective, true));
        }
        else
        {
          ValueBroadcast<LRBroadcast> *collective = 
            new ValueBroadcast<LRBroadcast>(this,logical_region_allocator_shard,
                                            COLLECTIVE_LOC_34);
          register_collective(collective);
          pending_region_trees.push_back(
              std::pair<ValueBroadcast<LRBroadcast>*,bool>(collective, false));
        }
        logical_region_allocator_shard++;
        if (logical_region_allocator_shard == total_shards)
          logical_region_allocator_shard = 0;
        double_next = false;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_shared_ownership(LogicalRegion handle)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_SHARED_OWNERSHIP);
        hasher.hash(handle);
        verify_replicable(hasher, "create_shared_ownership");
      }
      if (!handle.exists())
        return;
      if (!runtime->forest->is_top_level_region(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_SHARED_OWNERSHIP,
            "Illegal call to create shared ownership for logical region "
            "(%x,%x,%x in task %s (UID %lld) which is not a top-level logical "
            "region. Legion only permits top-level logical regions to have "
            "shared ownerships.", handle.index_space.id, handle.field_space.id,
            handle.tree_id, get_task_name(), get_unique_id())
      if (shard_manager->is_total_sharding() &&
          shard_manager->is_first_local_shard(owner_shard))
        runtime->create_shared_ownership(handle, true/*total sharding*/);
      else if (owner_shard->shard_id == 0)
        runtime->create_shared_ownership(handle);
      AutoLock priv_lock(privilege_lock);
      std::map<LogicalRegion,unsigned>::iterator finder = 
        created_regions.find(handle);
      if (finder != created_regions.end())
        finder->second++;
      else
        created_regions[handle] = 1;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_logical_region(LogicalRegion handle,
                                                  const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_LOGICAL_REGION);
        hasher.hash(handle);
        verify_replicable(hasher, "destroy_logical_region");
      }
      if (!handle.exists())
        return;
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_region.debug("Deleting logical region (%x,%x) in task %s (ID %lld)",
                         handle.index_space.id, handle.field_space.id, 
                         get_task_name(), get_unique_id());
#endif
      // Check to see if this is a top-level logical region, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_region(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy logical region (%x,%x,%x in task %s "
            "(UID %lld) which is not a top-level logical region. Legion only "
            "permits top-level logical regions to be destroyed.", 
            handle.index_space.id, handle.field_space.id, handle.tree_id,
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<LogicalRegion,unsigned>::iterator finder = 
          created_regions.find(handle);
        if (finder == created_regions.end())
        {
          // Check to see if it is a local region
          std::map<LogicalRegion,bool>::iterator local_finder = 
            local_regions.find(handle);
          // Mark that this region is deleted, safe even though this
          // is a read-only lock because we're not changing the structure
          // of the map
          if (local_finder == local_regions.end())
          {
            // Record the deletion for later and propagate it up
            deleted_regions.push_back(handle);
            return;
          }
          else
            local_finder->second = true;
        }
        else
        {
          if (finder->second == 0)
          {
            REPORT_LEGION_WARNING(LEGION_WARNING_DUPLICATE_DELETION,
                "Duplicate deletions were performed for region (%x,%x,%x) "
                "in task tree rooted by %s", handle.index_space.id, 
                handle.field_space.id, handle.tree_id, get_task_name())
            return;
          }
          if (--finder->second > 0)
            return;
          // Don't remove anything from created regions yet, we still might
          // need it as part of the logical dependence analysis for earlier
          // operations, but the reference count is zero so we're protected
        }
      }
      ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
      op->initialize_logical_region_deletion(this, handle, unordered);
      op->initialize_replication(this, deletion_ready_barrier,
          deletion_mapping_barrier, deletion_execution_barrier, 
          shard_manager->is_total_sharding(),
          shard_manager->is_first_local_shard(owner_shard));
      add_to_dependence_queue(op, unordered);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::advise_analysis_subtree(LogicalRegion parent,
                                   const std::set<LogicalRegion> &regions,
                                   const std::set<LogicalPartition> &partitions,
                                   const std::set<FieldID> &fields)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ADVISE_ANALYSIS_SUBTREE);
        hasher.hash(parent);
        for (std::set<LogicalRegion>::const_iterator it =
              regions.begin(); it != regions.end(); it++)
          hasher.hash(*it);
        for (std::set<LogicalPartition>::const_iterator it =
              partitions.begin(); it != partitions.end(); it++)
          hasher.hash(*it);
        for (std::set<FieldID>::const_iterator it =
              fields.begin(); it != fields.end(); it++)
          hasher.hash(*it);
        verify_replicable(hasher, "advise_analysis_subtree");
      }
      // Ignore advisement calls inside of traces replays
      if ((current_trace != NULL) && current_trace->is_fixed())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement was made inside of a trace.",
            get_task_name(), get_unique_id())
        return;
      }
      if (fields.empty())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement contains no fields.",
            get_task_name(), get_unique_id())
        return;
      }
      if (regions.empty() && partitions.empty())
      {
        REPORT_LEGION_WARNING(
            LEGION_WARNING_IGNORING_ADVISED_ANALYSIS_SUBTREE,
            "Ignoring advised analysis subtree in %s (UID %lld) because "
            "advisement contains no regions and partitions.",
            get_task_name(), get_unique_id())
        return;
      }
      AdvisementOp *advisement = runtime->get_available_advisement_op();
      advisement->initialize(this, parent, regions, partitions, fields,
          shard_manager->find_sharding_function(0/*default sharding*/));
      add_to_dependence_queue(advisement);
    }

    //--------------------------------------------------------------------------
    FieldAllocatorImpl* ReplicateContext::create_field_allocator(
                                              FieldSpace handle, bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_FIELD_ALLOCATOR);
        hasher.hash(handle);
        verify_replicable(hasher, "create_field_allocator");
      }
      {
        AutoLock priv_lock(privilege_lock,1,false/*exclusive*/);
        std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator finder = 
          field_allocators.find(handle);
        if (finder != field_allocators.end())
          return finder->second;
      }
      FieldSpaceNode *node = runtime->forest->get_node(handle);
      if (unordered)
      {
        // This next part is unsafe to perform in a control replicated
        // context if we are unordered, so just make a fresh allocator
        const RtEvent ready = node->create_allocator(runtime->address_space);
        // Don't have one so make a new one
        FieldAllocatorImpl *result = new FieldAllocatorImpl(node, this, ready);
        // DO NOT SAVE THIS!
        return result;
      }
      // Didn't find it, so have to make, retake the lock in exclusive mode
      AutoLock priv_lock(privilege_lock);
      // Check to see if we lost the race
      std::map<FieldSpace,FieldAllocatorImpl*>::const_iterator finder = 
        field_allocators.find(handle);
      if (finder != field_allocators.end())
        return finder->second;
      // Check to see which shard (if any) owns this field space
      const AddressSpaceID owner_space = 
        FieldSpaceNode::get_owner_space(handle, runtime);
      // Figure out which shard is the owner
      bool found = false;
      std::pair<ShardID,bool> owner(0,false);
      const ShardMapping &mapping = shard_manager->get_mapping();
      for (unsigned idx = 0; idx < mapping.size(); idx++)
      {
        if (mapping[idx] != owner_space)
          continue;
        owner.first = idx;
        found = true;
        break;
      }
      // Pick a shard to be the owner if we don't have a local shard
      if (!found)
      { 
        owner.first = field_allocator_shard++;
        if (field_allocator_shard == total_shards)
          field_allocator_shard = 0;
      }
      if (owner_space == runtime->address_space)
        owner.second = (owner.first == owner_shard->shard_id);
      else
        owner.second = shard_manager->is_first_local_shard(owner_shard);
#ifdef DEBUG_LEGION
      assert(field_allocator_owner_shards.find(handle) == 
              field_allocator_owner_shards.end());
#endif
      field_allocator_owner_shards[handle] = owner;
      RtEvent ready;
      if (owner.second)
        ready = node->create_allocator(runtime->address_space, 
            RtUserEvent::NO_RT_USER_EVENT, true/*sharded context*/, 
            (owner.first == owner_shard->shard_id));
      // Don't have one so make a new one
      FieldAllocatorImpl *result = new FieldAllocatorImpl(node, this, ready);
      // Save it for later
      field_allocators[handle] = result;
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_field_allocator(FieldSpaceNode *node)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_FIELD_ALLOCATOR);
        hasher.hash(node->handle);
        verify_replicable(hasher, "destroy_field_allocator");
      }
      bool found = false;
      std::pair<ShardID,bool> result;
      {
        AutoLock priv_lock(privilege_lock);
        // Check to see if we still have one
        std::map<FieldSpace,FieldAllocatorImpl*>::iterator finder = 
          field_allocators.find(node->handle);
        if (finder != field_allocators.end())
        {
          found = true;
          field_allocators.erase(finder);
          std::map<FieldSpace,std::pair<ShardID,bool> >::iterator owner_finder =
            field_allocator_owner_shards.find(node->handle);
#ifdef DEBUG_LEGION
          assert(owner_finder != field_allocator_owner_shards.end());
#endif
          result = owner_finder->second;
          
          field_allocator_owner_shards.erase(owner_finder);
        }
      }
      if (found && result.second)
      {
        const RtEvent ready = node->destroy_allocator(runtime->address_space,
              true/*sharded*/, (result.first == owner_shard->shard_id));
        if (ready.exists() && !ready.has_triggered())
          ready.wait();
      }
      else if (!found)
      {
        const RtEvent ready = node->destroy_allocator(runtime->address_space);
        if (ready.exists() && !ready.has_triggered())
          ready.wait();
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::insert_unordered_ops(AutoLock &d_lock,
                                       const bool end_task, const bool progress)
    //--------------------------------------------------------------------------
    {
      // If we have a trace then we're definitely not inserting operations
      if (current_trace != NULL)
        return;
      // For control replication, we need to have an algorithm to determine
      // when the shards try to sync up to insert operations that doesn't
      // rely on knowing if or when any one shard has unordered ops
      // We employ a sampling based algorithm here with exponential backoff
      // to detect when we are doing unordered ops since it's likely a 
      // binary state where either we are or we aren't doing unordered ops
      if (!end_task)
      {
#ifdef DEBUG_LEGION
        assert(unordered_ops_counter < unordered_ops_epoch);
#endif
        // If we're doing progress then we can skip this check and
        // reset the counter back to zero since we're doing an exchange
        if (!progress)
        {
          if (++unordered_ops_counter < unordered_ops_epoch)
            return;
        }
        else
          unordered_ops_counter = 0;
      }
      // If we're at the end of the task and we don't have any unordered ops
      // then nobody else should have any either so we are done
      else if (unordered_ops.empty())
        return;
      // If we make it here then all the shards are agreed that they are
      // going to do the sync up and will exchange information 
      // We're going to release the lock so we need to grab a local copy
      // of all our unordered operations.
      std::list<Operation*> local_unordered;
      local_unordered.swap(unordered_ops);
      // Now we can release the lock and do the exchange
      d_lock.release();
      UnorderedExchange exchange(this, COLLECTIVE_LOC_88); 
      std::vector<Operation*> ready_ops;
      const bool any_unordered_ops = 
        exchange.exchange_unordered_ops(local_unordered, ready_ops);
      // Reacquire the lock and handle the operations
      d_lock.reacquire();
      if (!ready_ops.empty())
      {
        for (std::vector<Operation*>::const_iterator it = 
              ready_ops.begin(); it != ready_ops.end(); it++)
        {
          (*it)->set_tracking_parent(total_children_count++);
          dependence_queue.push_back(*it);
        }
        __sync_fetch_and_add(&outstanding_children_count, ready_ops.size());
        if (ready_ops.size() != local_unordered.size())
        {
          // For any operations which we aren't in the ready ops
          // then we need to put them back on the unordered list
          for (std::list<Operation*>::const_reverse_iterator it = 
                local_unordered.rbegin(); it != local_unordered.rend(); it++)
          {
            bool found = false;
            for (unsigned idx = 0; idx < ready_ops.size(); idx++)
            {
              if (ready_ops[idx] != (*it))
                continue;
              found = true;
              break;
            }
            if (!found)
              unordered_ops.push_front(*it);
          }
        }
      }
      else if (!local_unordered.empty())
      {
        // Put all our of items back on the unordered list
        if (!unordered_ops.empty())
          unordered_ops.insert(unordered_ops.begin(),
              local_unordered.begin(), local_unordered.end());
        else
          unordered_ops.swap(local_unordered);
      }
      if (!end_task)
      {
        // Reset the count
        unordered_ops_counter = 0;
        // Check to see how to adjust the epoch size
        if (!any_unordered_ops)
        {
          // If there were no ready unordered ops then we double the epoch
          if (unordered_ops_epoch < MAX_UNORDERED_OPS_EPOCH)
            unordered_ops_epoch *= 2;
        }
        else // Otherwise reset to min epoch size
          unordered_ops_epoch = MIN_UNORDERED_OPS_EPOCH;
#ifdef DEBUG_LEGION
        assert(MIN_UNORDERED_OPS_EPOCH <= unordered_ops_epoch);
        assert(unordered_ops_epoch <= MAX_UNORDERED_OPS_EPOCH);
#endif
      }
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::execute_task(const TaskLauncher &launcher,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_EXECUTE_TASK);
        hash_task_launcher(hasher, runtime->safe_control_replication, launcher);
        verify_replicable(hasher, "execute_task");
      }
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_task_false(launcher);
      // If we're doing a local-function task then we can run that with just
      // a normal individual task in each shard since it is safe to duplicate
      if (launcher.local_function_task)
        return InnerContext::execute_task(launcher, outputs);
      ReplIndividualTask *task = 
        runtime->get_available_repl_individual_task();
      Future result = task->initialize_task(this,
                                            launcher,
                                            true /*track*/,
                                            false /*top_level*/,
                                            false /*implicit_top_level*/,
                                            outputs);
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_task.debug("Registering new single task with unique id %lld "
                        "and task %s (ID %lld) with high level runtime in "
                        "addresss space %d",
                        task->get_unique_id(), task->get_task_name(), 
                        task->get_unique_id(), runtime->address_space);
      task->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_43));
#endif
      // Now initialize the particular information for replication 
      task->initialize_replication(this);
      if (launcher.enable_inlining && !launcher.silence_warnings)
        REPORT_LEGION_WARNING(LEGION_WARNING_INLINING_NOT_SUPPORTED,
            "Inlining is not currently supported for replicated tasks "
            "such as %s (UID %lld)", get_task_name(), get_unique_id())
      execute_task_launch(task, false/*index*/, current_trace, 
                          launcher.silence_warnings, false/*no inlining*/);
      return result;
    }

    //--------------------------------------------------------------------------
    FutureMap ReplicateContext::execute_index_space(
                                        const IndexTaskLauncher &launcher,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (launcher.must_parallelism)
      {
        // Turn around and use a must epoch launcher
        MustEpochLauncher epoch_launcher(launcher.map_id, launcher.tag);
        epoch_launcher.add_index_task(launcher);
        FutureMap result = execute_must_epoch(epoch_launcher);
        return result;
      }
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_EXECUTE_INDEX_SPACE);
        hash_index_launcher(hasher, runtime->safe_control_replication,launcher);
        verify_replicable(hasher, "execute_index_space");
      }
      if (launcher.launch_domain.exists() && 
          (launcher.launch_domain.get_volume() == 0))
      {
        log_run.warning("Ignoring empty index task launch in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return FutureMap();
      }
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_index_task_false(
            __sync_add_and_fetch(&outstanding_children_count,1), launcher);
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      ReplIndexTask *task = runtime->get_available_repl_index_task();
      FutureMap result = task->initialize_task(this,
                                               launcher,
                                               launch_space,
                                               true /*track*/,
                                               outputs);
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_task.debug("Registering new index space task with unique id "
                       "%lld and task %s (ID %lld) with high level runtime in "
                       "address space %d",
                       task->get_unique_id(), task->get_task_name(), 
                       task->get_unique_id(), runtime->address_space);
      task->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_44));
#endif
      task->initialize_replication(this);
      if (launcher.enable_inlining && !launcher.silence_warnings)
        REPORT_LEGION_WARNING(LEGION_WARNING_INLINING_NOT_SUPPORTED,
            "Inlining is not currently supported for replicated tasks "
            "such as %s (UID %lld)", get_task_name(), get_unique_id())
      execute_task_launch(task, true/*index*/, current_trace, 
                          launcher.silence_warnings, false/*no inlining*/);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::execute_index_space(
                                        const IndexTaskLauncher &launcher,
                                        ReductionOpID redop, bool deterministic,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (launcher.must_parallelism)
        REPORT_LEGION_FATAL(LEGION_FATAL_UNIMPLEMENTED_FEATURE,
            "Task %s (UID %lld) requested an index space launch with must "
            "parallelism (aka a MustEpochLaunch) that needs a reduction of "
            "all future values. This feature is not currently implemented.",
            get_task_name(), get_unique_id())
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_EXECUTE_INDEX_SPACE);
        hash_index_launcher(hasher, runtime->safe_control_replication,launcher);
        hasher.hash(redop);
        hasher.hash<bool>(deterministic);
        verify_replicable(hasher, "execute_index_space");
      }
      // Quick out for predicate false
      if (launcher.predicate == Predicate::FALSE_PRED)
        return predicate_index_task_reduce_false(launcher);
      if (launcher.launch_domain.exists() &&
          (launcher.launch_domain.get_volume() == 0))
      {
        log_run.warning("Ignoring empty index task launch in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return Future();
      }
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      ReplIndexTask *task = runtime->get_available_repl_index_task();
      Future result = task->initialize_task(this, launcher, launch_space, 
                                            redop, deterministic, true/*track*/,
                                            outputs);
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_task.debug("Registering new index space task with unique id "
                       "%lld and task %s (ID %lld) with high level runtime in "
                       "address space %d",
                       task->get_unique_id(), task->get_task_name(), 
                       task->get_unique_id(), runtime->address_space);
      task->set_sharding_collective(new ShardingGatherCollective(this, 
                                    0/*owner shard*/, COLLECTIVE_LOC_45));
#endif
      task->initialize_replication(this);
      if (launcher.enable_inlining && !launcher.silence_warnings)
        REPORT_LEGION_WARNING(LEGION_WARNING_INLINING_NOT_SUPPORTED,
            "Inlining is not currently supported for replicated tasks "
            "such as %s (UID %lld)", get_task_name(), get_unique_id())
      execute_task_launch(task, true/*index*/, current_trace, 
                          launcher.silence_warnings, false/*no inlining*/);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::reduce_future_map(const FutureMap &future_map,
                                        ReductionOpID redop, bool deterministic,
                                        MapperID mapper_id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this); 
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_REDUCE_FUTURE_MAP);
        hash_future_map(hasher, future_map);
        hasher.hash(redop);
        hasher.hash(deterministic);
        verify_replicable(hasher, "reduce_future_map");
      }
      if (future_map.impl == NULL)
        return Future();
      // Check to see if this is just a normal future map, if so then 
      // we can just do the standard thing here
      if (!future_map.impl->is_replicate_future_map())
        return InnerContext::reduce_future_map(future_map, redop,
                                               deterministic, mapper_id, tag);
      ReplAllReduceOp *all_reduce_op = 
        runtime->get_available_repl_all_reduce_op();
      Future result = all_reduce_op->initialize(this, future_map, redop, 
                                                deterministic, mapper_id, tag);
      all_reduce_op->initialize_replication(this);
      add_to_dependence_queue(all_reduce_op);
      return result;
    }

    //--------------------------------------------------------------------------
    PhysicalRegion ReplicateContext::map_region(const InlineLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_MAP_REGION);
        Serializer rez;
        ExternalMappable::pack_region_requirement(launcher.requirement, rez);
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        hash_grants(hasher, launcher.grants);
        hash_phase_barriers(hasher, launcher.wait_barriers);
        hash_phase_barriers(hasher, launcher.arrive_barriers);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.tag);
        hasher.hash(launcher.layout_constraint_id);
        hash_static_dependences(hasher, launcher.static_dependences);
        verify_replicable(hasher, "map_region");
      }
      if (IS_NO_ACCESS(launcher.requirement))
        return PhysicalRegion();
      ReplMapOp *map_op = runtime->get_available_repl_map_op();
      PhysicalRegion result = map_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Registering a map operation for region "
                    "(%x,%x,%x) in task %s (ID %lld)",
                    launcher.requirement.region.index_space.id, 
                    launcher.requirement.region.field_space.id, 
                    launcher.requirement.region.tree_id, 
                    get_task_name(), get_unique_id());
#endif
      map_op->initialize_replication(this, inline_mapping_barrier);
   
      bool parent_conflict = false, inline_conflict = false;  
      const int index = 
        has_conflicting_regions(map_op, parent_conflict, inline_conflict);
      if (parent_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_INLINE_MAPPING_REGION,
          "Attempted an inline mapping of region "
                      "(%x,%x,%x) that conflicts with mapped region " 
                      "(%x,%x,%x) at index %d of parent task %s "
                      "(ID %lld) that would ultimately result in "
                      "deadlock. Instead you receive this error message.",
                      launcher.requirement.region.index_space.id,
                      launcher.requirement.region.field_space.id,
                      launcher.requirement.region.tree_id,
                      regions[index].region.index_space.id,
                      regions[index].region.field_space.id,
                      regions[index].region.tree_id,
                      index, get_task_name(), get_unique_id())
      if (inline_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_INLINE_MAPPING_REGION,
          "Attempted an inline mapping of region (%x,%x,%x) "
                      "that conflicts with previous inline mapping in "
                      "task %s (ID %lld) that would ultimately result in "
                      "deadlock.  Instead you receive this error message.",
                      launcher.requirement.region.index_space.id,
                      launcher.requirement.region.field_space.id,
                      launcher.requirement.region.tree_id,
                      get_task_name(), get_unique_id())
      register_inline_mapped_region(result);
      add_to_dependence_queue(map_op);
      return result;
    }

    //--------------------------------------------------------------------------
    ApEvent ReplicateContext::remap_region(PhysicalRegion region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_REMAP_REGION);
        Serializer rez;
        ExternalMappable::pack_region_requirement(
            region.impl->get_requirement(), rez);
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        hasher.hash<bool>(region.is_mapped());
        verify_replicable(hasher, "remap_region");
      }
      // Check to see if the region is already mapped,
      // if it is then we are done
      if (region.is_mapped())
        return ApEvent::NO_AP_EVENT;
      ReplMapOp *map_op = runtime->get_available_repl_map_op();
      map_op->initialize(this, region);
      map_op->initialize_replication(this, inline_mapping_barrier);
      register_inline_mapped_region(region);
      const ApEvent result = map_op->get_completion_event();
      add_to_dependence_queue(map_op);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::fill_fields(const FillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_FILL_FIELDS);
        hasher.hash(launcher.handle);
        hasher.hash(launcher.parent);
        hash_argument(hasher, runtime->safe_control_replication, 
                      launcher.argument);
        hash_future(hasher, runtime->safe_control_replication, launcher.future);
        hash_predicate(hasher, launcher.predicate);
        for (std::set<FieldID>::const_iterator it = 
              launcher.fields.begin(); it != launcher.fields.end(); it++)
          hasher.hash(*it);
        hash_grants(hasher, launcher.grants);
        hash_phase_barriers(hasher, launcher.wait_barriers);
        hash_phase_barriers(hasher, launcher.arrive_barriers);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.tag);
        for (int idx = 0; idx < launcher.point.get_dim(); idx++)
          hasher.hash(launcher.point[idx]);
        hasher.hash(launcher.sharding_space);
        hash_static_dependences(hasher, launcher.static_dependences);
        hasher.hash(launcher.silence_warnings);
        verify_replicable(hasher, "fill_fields");
      }
      if (launcher.fields.empty())
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_EMPTY_FILL_FIELDS,
            "Ignoring fill request with no fields in ask %s (UID %lld)",
            get_task_name(), get_unique_id())
        return;
      }
      ReplFillOp *fill_op = runtime->get_available_repl_fill_op();
      fill_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      log_run.debug("Registering a fill operation in task %s (ID %lld)",
                     get_task_name(), get_unique_id());
      fill_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                       0/*owner shard*/, COLLECTIVE_LOC_51));
#endif
      fill_op->initialize_replication(this);
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(fill_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
        {
          REPORT_LEGION_WARNING(LEGION_WARNING_RUNTIME_UNMAPPING_REMAPPING,
            "WARNING: Runtime is unmapping and remapping "
              "physical regions around fill_fields call in task %s (UID %lld).",
              get_task_name(), get_unique_id());
        }
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(fill_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::fill_fields(const IndexFillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_FILL_FIELDS);
        hasher.hash(launcher.launch_domain);
        hasher.hash(launcher.launch_space);
        hasher.hash(launcher.sharding_space);
        hasher.hash(launcher.region);
        hasher.hash(launcher.partition);
        hasher.hash(launcher.parent);
        hasher.hash(launcher.projection);
        hash_argument(hasher, runtime->safe_control_replication, 
                      launcher.argument);
        hash_future(hasher, runtime->safe_control_replication, launcher.future);
        hash_predicate(hasher, launcher.predicate);
        for (std::set<FieldID>::const_iterator it = 
              launcher.fields.begin(); it != launcher.fields.end(); it++)
          hasher.hash(*it);
        hash_grants(hasher, launcher.grants);
        hash_phase_barriers(hasher, launcher.wait_barriers);
        hash_phase_barriers(hasher, launcher.arrive_barriers);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.tag);
        hash_static_dependences(hasher, launcher.static_dependences);
        hasher.hash(launcher.silence_warnings);
        verify_replicable(hasher, "fill_fields");
      }
      if (launcher.fields.empty())
      {
        REPORT_LEGION_WARNING(LEGION_WARNING_EMPTY_FILL_FIELDS,
            "Ignoring index fill request with no fields in ask %s (UID %lld)",
            get_task_name(), get_unique_id())
        return;
      }
      if (launcher.launch_domain.exists() && 
          (launcher.launch_domain.get_volume() == 0))
      {
        log_run.warning("Ignoring empty index space fill in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
        return;
      }
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      ReplIndexFillOp *fill_op = 
        runtime->get_available_repl_index_fill_op();
      fill_op->initialize(this, launcher, launch_space); 
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Registering an index fill operation in task %s "
                      "(ID %lld)", get_task_name(), get_unique_id());
      fill_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                       0/*owner shard*/, COLLECTIVE_LOC_46));
#endif
      fill_op->initialize_replication(this);
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(fill_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around fill_fields call in task %s (UID %lld).",
              get_task_name(), get_unique_id());
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(fill_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::issue_copy(const CopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ISSUE_COPY);
        hash_region_requirements(hasher, launcher.src_requirements);
        hash_region_requirements(hasher, launcher.dst_requirements);
        hash_region_requirements(hasher, launcher.src_indirect_requirements);
        hash_region_requirements(hasher, launcher.dst_indirect_requirements);
        for (std::vector<bool>::const_iterator it = 
              launcher.src_indirect_is_range.begin(); it != 
              launcher.src_indirect_is_range.end(); it++)
          hasher.hash<bool>(*it);
        for (std::vector<bool>::const_iterator it = 
              launcher.dst_indirect_is_range.begin(); it != 
              launcher.dst_indirect_is_range.end(); it++)
          hasher.hash<bool>(*it);
        hash_grants(hasher, launcher.grants);
        hash_phase_barriers(hasher, launcher.wait_barriers);
        hash_phase_barriers(hasher, launcher.arrive_barriers);
        hash_predicate(hasher, launcher.predicate);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.tag);
        for (int idx = 0; idx < launcher.point.get_dim(); idx++)
          hasher.hash(launcher.point[idx]);
        hasher.hash(launcher.sharding_space);
        hash_static_dependences(hasher, launcher.static_dependences);
        hasher.hash(launcher.possible_src_indirect_out_of_range);
        hasher.hash(launcher.possible_dst_indirect_out_of_range);
        hasher.hash(launcher.possible_dst_indirect_aliasing);
        hasher.hash(launcher.silence_warnings);
        verify_replicable(hasher, "issue_copy"); 
      }
      ReplCopyOp *copy_op = runtime->get_available_repl_copy_op();
      copy_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Registering a copy operation in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
      copy_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                       0/*owner shard*/, COLLECTIVE_LOC_47));
#endif
      copy_op->initialize_replication(this);
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(copy_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around issue_copy_operation call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(copy_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    } 
    
    //--------------------------------------------------------------------------
    void ReplicateContext::issue_copy(const IndexCopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ISSUE_COPY);
        hash_region_requirements(hasher, launcher.src_requirements);
        hash_region_requirements(hasher, launcher.dst_requirements);
        hash_region_requirements(hasher, launcher.src_indirect_requirements);
        hash_region_requirements(hasher, launcher.dst_indirect_requirements);
        for (std::vector<bool>::const_iterator it = 
              launcher.src_indirect_is_range.begin(); it != 
              launcher.src_indirect_is_range.end(); it++)
          hasher.hash<bool>(*it);
        for (std::vector<bool>::const_iterator it = 
              launcher.dst_indirect_is_range.begin(); it != 
              launcher.dst_indirect_is_range.end(); it++)
          hasher.hash<bool>(*it);
        hash_grants(hasher, launcher.grants);
        hash_phase_barriers(hasher, launcher.wait_barriers);
        hash_phase_barriers(hasher, launcher.arrive_barriers);
        hash_predicate(hasher, launcher.predicate);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.tag);
        hasher.hash(launcher.launch_domain);
        hasher.hash(launcher.launch_space);
        hasher.hash(launcher.sharding_space);
        hash_static_dependences(hasher, launcher.static_dependences);
        hasher.hash(launcher.possible_src_indirect_out_of_range);
        hasher.hash(launcher.possible_dst_indirect_out_of_range);
        hasher.hash(launcher.possible_dst_indirect_aliasing);
        hasher.hash(launcher.collective_src_indirect_points);
        hasher.hash(launcher.collective_dst_indirect_points);
        hasher.hash(launcher.silence_warnings);
        verify_replicable(hasher, "issue_copy");
      }
      if (launcher.launch_domain.exists() &&
          (launcher.launch_domain.get_volume() == 0))
      {
        log_run.warning("Ignoring empty index space copy in task %s "
                        "(ID %lld)", get_task_name(), get_unique_id());
        return;
      }
      IndexSpace launch_space = launcher.launch_space;
      if (!launch_space.exists())
        launch_space = find_index_launch_space(launcher.launch_domain);
      ReplIndexCopyOp *copy_op = 
        runtime->get_available_repl_index_copy_op();
      copy_op->initialize(this, launcher, launch_space); 
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Registering an index copy operation in task %s "
                      "(ID %lld)", get_task_name(), get_unique_id());
      copy_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                       0/*owner shard*/, COLLECTIVE_LOC_48));
#endif
      copy_op->initialize_replication(this, indirection_barriers,
                                      next_indirection_bar_index);
      // Check to see if we need to do any unmappings and remappings
      // before we can issue this copy operation
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        find_conflicting_regions(copy_op, unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around issue_copy_operation call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        // Unmap any regions which are conflicting
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Issue the copy operation
      add_to_dependence_queue(copy_op);
      // Remap any regions which we unmapped
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::issue_acquire(const AcquireLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Acquire operations are not currently supported in control "
                    "replication contexts for task %s (UID %lld). It may be "
                    "supported in the future.",
                    get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::issue_release(const ReleaseLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Release operations are not currently supported in control "
                    "replication contexts for task %s (UID %lld). It may be "
                    "supported in the future.",
                    get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    PhysicalRegion ReplicateContext::attach_resource(
                                                 const AttachLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ATTACH_RESOURCE);
        hasher.hash(launcher.resource);
        hasher.hash(launcher.handle);
        hasher.hash(launcher.parent);
        hasher.hash(launcher.restricted);
        hasher.hash(launcher.mapped);
        if (launcher.file_name != NULL)
          hasher.hash(launcher.file_name, strlen(launcher.file_name));
        hasher.hash(launcher.mode);
        for (std::vector<FieldID>::const_iterator it = 
              launcher.file_fields.begin(); it != 
              launcher.file_fields.end(); it++)
          hasher.hash(*it);
        for (std::map<FieldID,const char*>::const_iterator it = 
              launcher.field_files.begin(); it != 
              launcher.field_files.end(); it++)
        {
          hasher.hash(it->first);
          hasher.hash(it->second, strlen(it->second));
        }
        hasher.hash(launcher.local_files);
        Serializer rez;
        launcher.constraints.serialize(rez);
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        for (std::set<FieldID>::const_iterator it = 
              launcher.privilege_fields.begin(); it !=
              launcher.privilege_fields.end(); it++)
          hasher.hash(*it);
        hasher.hash(launcher.footprint);
        hash_static_dependences(hasher, launcher.static_dependences);
        verify_replicable(hasher, "attach_resource");
      }
      if (launcher.restricted)
        REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
            "Attach operations in control replication context %s (UID %lld) "
            "requested a restriction. Restrictions are only permitted for "
            "attach operations in non-control-replicated contexts currently.",
            get_task_name(), get_unique_id());
      ReplAttachOp *attach_op = runtime->get_available_repl_attach_op();
      PhysicalRegion result = attach_op->initialize(this, launcher);
      attach_op->initialize_replication(this, external_resource_barrier,
                        attach_broadcast_barrier, attach_reduce_barrier);

      bool parent_conflict = false, inline_conflict = false;
      int index = has_conflicting_regions(attach_op, 
                                          parent_conflict, inline_conflict);
      if (parent_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_ATTACH_HDF5,
          "Attempted an attach hdf5 file operation on region "
                      "(%x,%x,%x) that conflicts with mapped region " 
                      "(%x,%x,%x) at index %d of parent task %s (ID %lld) "
                      "that would ultimately result in deadlock. Instead you "
                      "receive this error message. Try unmapping the region "
                      "before invoking attach_hdf5 on file %s",
                      launcher.handle.index_space.id, 
                      launcher.handle.field_space.id, 
                      launcher.handle.tree_id, 
                      regions[index].region.index_space.id,
                      regions[index].region.field_space.id,
                      regions[index].region.tree_id, index, 
                      get_task_name(), get_unique_id(), launcher.file_name)
      if (inline_conflict)
        REPORT_LEGION_ERROR(ERROR_ATTEMPTED_ATTACH_HDF5,
          "Attempted an attach hdf5 file operation on region "
                      "(%x,%x,%x) that conflicts with previous inline "
                      "mapping in task %s (ID %lld) "
                      "that would ultimately result in deadlock. Instead you "
                      "receive this error message. Try unmapping the region "
                      "before invoking attach_hdf5 on file %s",
                      launcher.handle.index_space.id, 
                      launcher.handle.field_space.id, 
                      launcher.handle.tree_id, get_task_name(), 
                      get_unique_id(), launcher.file_name)
      // If we're counting this region as mapped we need to register it
      if (launcher.mapped)
        register_inline_mapped_region(result);
      add_to_dependence_queue(attach_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::detach_resource(PhysicalRegion region, 
                                         const bool flush, const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DETACH_RESOURCE);
        Serializer rez;
        ExternalMappable::pack_region_requirement(
            region.impl->get_requirement(), rez);
        hasher.hash(rez.get_buffer(), rez.get_used_bytes());
        hasher.hash<bool>(region.is_mapped());
        hasher.hash<bool>(flush);
        verify_replicable(hasher, "detach_resource"); 
      }
      ReplDetachOp *op = runtime->get_available_repl_detach_op();
      Future result = op->initialize_detach(this, region, flush, unordered);
      op->initialize_replication(this, external_resource_barrier); 
      // If the region is still mapped, then unmap it
      if (region.is_mapped())
      {
        unregister_inline_mapped_region(region);
        region.impl->unmap_region();
      }
      add_to_dependence_queue(op, unordered);
      return result;
    }

    //--------------------------------------------------------------------------
    FutureMap ReplicateContext::execute_must_epoch(
                                              const MustEpochLauncher &launcher)
    //--------------------------------------------------------------------------
    {
#ifdef SAFE_MUST_EPOCH_LAUNCHES
      // See the comment in InnerContext::execute_must_epoch for why this
      // particular call is here for safe must epoch launches
      // Also see github issue #659
      issue_execution_fence(); 
#endif
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && 
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_MUST_EPOCH);
        hasher.hash(launcher.map_id);
        hasher.hash(launcher.mapping_tag);
        for (std::vector<TaskLauncher>::const_iterator it = 
              launcher.single_tasks.begin(); it != 
              launcher.single_tasks.end(); it++)
          hash_task_launcher(hasher, runtime->safe_control_replication, *it);
        for (std::vector<IndexTaskLauncher>::const_iterator it = 
              launcher.index_tasks.begin(); it !=
              launcher.index_tasks.end(); it++)
          hash_index_launcher(hasher, runtime->safe_control_replication, *it);
        hasher.hash(launcher.launch_domain);
        hasher.hash(launcher.launch_space);
        hasher.hash(launcher.sharding_space);
        hasher.hash(launcher.silence_warnings);
        verify_replicable(hasher, "execute_must_epoch");
      }
      ReplMustEpochOp *epoch_op = runtime->get_available_repl_epoch_op();
      FutureMap result = epoch_op->initialize(this, launcher);
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Executing a must epoch in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
      epoch_op->set_sharding_collective(new ShardingGatherCollective(this, 
                                        0/*owner shard*/, COLLECTIVE_LOC_49));
#endif
      epoch_op->initialize_replication(this);
      // Now find all the parent task regions we need to invalidate
      std::vector<PhysicalRegion> unmapped_regions;
      if (!runtime->unsafe_launch)
        epoch_op->find_conflicted_regions(unmapped_regions);
      if (!unmapped_regions.empty())
      {
        if (runtime->runtime_warnings && !launcher.silence_warnings)
          log_run.warning("WARNING: Runtime is unmapping and remapping "
              "physical regions around issue_release call in "
              "task %s (UID %lld).", get_task_name(), get_unique_id());
        for (unsigned idx = 0; idx < unmapped_regions.size(); idx++)
          unmapped_regions[idx].impl->unmap_region();
      }
      // Now we can issue the must epoch
      add_to_dependence_queue(epoch_op);
      // Remap any unmapped regions
      if (!unmapped_regions.empty())
        remap_unmapped_regions(current_trace, unmapped_regions);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::issue_timing_measurement(
                                                 const TimingLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_TIMING_MEASUREMENT);
        hasher.hash(launcher.measurement);
        for (std::set<Future>::const_iterator it = 
              launcher.preconditions.begin(); it !=
              launcher.preconditions.end(); it++)
          hash_future(hasher, runtime->safe_control_replication, *it);
        verify_replicable(hasher, "issue_timing_measurement");
      }
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Issuing a timing measurement in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      ReplTimingOp *timing_op = runtime->get_available_repl_timing_op();
      Future result = timing_op->initialize(this, launcher);
      ValueBroadcast<long long> *timing_collective = 
        new ValueBroadcast<long long>(this, 0/*shard 0 is always the owner*/,
                                      COLLECTIVE_LOC_35);
      timing_op->set_timing_collective(timing_collective);
      add_to_dependence_queue(timing_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::issue_mapping_fence(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && 
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_MAPPING_FENCE);
        verify_replicable(hasher, "issue_mapping_fence");
      }
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Issuing a mapping fence in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      ReplFenceOp *fence_op = runtime->get_available_repl_fence_op();
      Future result = fence_op->initialize(this, FenceOp::MAPPING_FENCE, true);
      add_to_dependence_queue(fence_op);
      return result;
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::issue_execution_fence(void)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_EXECUTION_FENCE);
        verify_replicable(hasher, "issue_execution_fence");
      }
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Issuing an execution fence in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      ReplFenceOp *fence_op = runtime->get_available_repl_fence_op();
      Future result = fence_op->initialize(this, FenceOp::EXECUTION_FENCE,true);
      add_to_dependence_queue(fence_op);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::begin_trace(TraceID tid, bool logical_only,
        bool static_trace, const std::set<RegionTreeID> *trees, bool deprecated)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication)
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_BEGIN_TRACE);
        hasher.hash(tid);
        hasher.hash<bool>(logical_only);
        hasher.hash<bool>(static_trace);
        hasher.hash<bool>(deprecated);
        if (trees != NULL)
          for (std::set<RegionTreeID>::const_iterator it = 
                trees->begin(); it != trees->end(); it++)
            hasher.hash(*it);
        verify_replicable(hasher, "begin_trace");
      }
      if (runtime->no_tracing) return;
      if (runtime->no_physical_tracing) logical_only = true;
#ifdef DEBUG_LEGION
      log_run.debug("Beginning a trace in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      // No need to hold the lock here, this is only ever called
      // by the one thread that is running the task.
      if (current_trace != NULL)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_NESTED_TRACE,
          "Illegal nested trace with ID %d attempted in "
           "task %s (ID %lld)", tid, get_task_name(), get_unique_id())
      std::map<TraceID,LegionTrace*>::const_iterator finder = traces.find(tid);
      LegionTrace *trace = NULL;
      if (finder == traces.end())
      {
        // Trace does not exist yet, so make one and record it
        if (static_trace)
          trace = new StaticTrace(tid, this, logical_only, trees);
        else
          trace = new DynamicTrace(tid, this, logical_only);
        if (!deprecated)
          traces[tid] = trace;
        trace->add_reference();
      }
      else
        trace = finder->second;

#ifdef DEBUG_LEGION
      assert(trace != NULL);
#endif
      trace->clear_blocking_call();

      // Issue a begin op
      ReplTraceBeginOp *begin = runtime->get_available_repl_begin_op();
      begin->initialize_begin(this, trace);
      add_to_dependence_queue(begin);

      if (!logical_only)
      {
        // Issue a replay op
        ReplTraceReplayOp *replay = runtime->get_available_repl_replay_op();
        replay->initialize_replay(this, trace);
        add_to_dependence_queue(replay);
      }

      // Now mark that we are starting a trace
      current_trace = trace;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::end_trace(TraceID tid, bool deprecated)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication)
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_END_TRACE);
        hasher.hash(tid);
        hasher.hash<bool>(deprecated);
        verify_replicable(hasher, "end_trace");
      }
      if (runtime->no_tracing) return;
#ifdef DEBUG_LEGION
      log_run.debug("Ending a trace in task %s (ID %lld)",
                    get_task_name(), get_unique_id());
#endif
      if (current_trace == NULL)
        REPORT_LEGION_ERROR(ERROR_UMATCHED_END_TRACE,
          "Unmatched end trace for ID %d in task %s (ID %lld)", 
          tid, get_task_name(), get_unique_id())
      else if (!deprecated && (current_trace->tid != tid))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_END_TRACE_CALL,
          "Illegal end trace call on trace ID %d that does not match "
          "the current trace ID %d in task %s (UID %lld)", tid,
          current_trace->tid, get_task_name(), get_unique_id())
      const bool has_blocking_call = current_trace->has_blocking_call();
      if (current_trace->is_fixed())
      {
        // Already fixed, dump a complete trace op into the stream
        ReplTraceCompleteOp *complete_op = 
          runtime->get_available_repl_trace_op();
        complete_op->initialize_complete(this, has_blocking_call);
        add_to_dependence_queue(complete_op);
      }
      else
      {
        // Not fixed yet, dump a capture trace op into the stream
        ReplTraceCaptureOp *capture_op = 
          runtime->get_available_repl_capture_op();
        capture_op->initialize_capture(this, has_blocking_call, deprecated);
        // Mark that the current trace is now fixed
        current_trace->fix_trace();
        add_to_dependence_queue(capture_op);
      }
      // We no longer have a trace that we're executing 
      current_trace = NULL;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::end_task(const void *res, size_t res_size,bool owned,
     PhysicalInstance deferred_result_instance, FutureFunctor *callback_functor,
                       Memory::Kind result_kind, void (*freefunc)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      // We have an extra one of these here to handle the case where some
      // shards do an extra runtime call than other shards. This should
      // avoid that case hanging at least.
      if (runtime->safe_control_replication)
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_END_TASK);
        hasher.hash(res_size);
        verify_replicable(hasher, "end_task");
      }
      InnerContext::end_task(res, res_size, owned, deferred_result_instance, 
                             callback_functor, result_kind, freefunc);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::post_end_task(FutureInstance *instance,
                                         FutureFunctor *callback_functor,
                                         bool own_callback_functor)
    //--------------------------------------------------------------------------
    {
      // Pull any pending collectives here on the stack so we can delete them
      // after the end task call, even though this context might be reclaimed
      std::deque<std::pair<ValueBroadcast<ISBroadcast>*,bool> > 
                                            release_index_spaces;
      if (!pending_index_spaces.empty())
        release_index_spaces.swap(pending_index_spaces);
      std::deque<std::pair<ValueBroadcast<IPBroadcast>*,ShardID> >
                                            release_index_partitions;
      if (!pending_index_partitions.empty())
        release_index_partitions.swap(pending_index_partitions);
      std::deque<std::pair<ValueBroadcast<FSBroadcast>*,bool> >
                                            release_field_spaces;
      if (!pending_field_spaces.empty())
        release_field_spaces.swap(pending_field_spaces);
      std::deque<std::pair<ValueBroadcast<FIDBroadcast>*,bool> >
                                            release_fields;
      if (!pending_fields.empty())
        release_fields.swap(pending_fields);
      std::deque<std::pair<ValueBroadcast<LRBroadcast>*,bool> >
                                            release_region_trees;
      if (!pending_region_trees.empty())
        release_region_trees.swap(pending_region_trees);
      // Grab this now before the context might be deleted
      const ShardID local_shard = owner_shard->shard_id;
      // Do the base call
      InnerContext::post_end_task(instance, callback_functor,
                                  own_callback_functor);
      // Then delete all the pending collectives that we had
      while (!release_index_spaces.empty())
      {
        std::pair<ValueBroadcast<ISBroadcast>*,bool> &collective = 
          release_index_spaces.front();
        if (collective.second)
        {
          const ISBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_index_space(value.space_id);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        release_index_spaces.pop_front();
      }
      while (!release_index_partitions.empty())
      {
        std::pair<ValueBroadcast<IPBroadcast>*,ShardID> &collective = 
          release_index_partitions.front();
        if (collective.second == local_shard)
        {
          const IPBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_partition(value.pid);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        release_index_partitions.pop_front();
      }
      while (!release_field_spaces.empty())
      {
        std::pair<ValueBroadcast<FSBroadcast>*,bool> &collective = 
          release_field_spaces.front();
        if (collective.second)
        {
          const FSBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_field_space(value.space_id);
          runtime->revoke_pending_distributed_collectable(value.did);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        release_field_spaces.pop_front();
      }
      while (!release_fields.empty())
      {
        std::pair<ValueBroadcast<FIDBroadcast>*,bool> &collective = 
          release_fields.front();
        if (!collective.second)
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        release_fields.pop_front();
      }
      while (!release_region_trees.empty())
      {
        std::pair<ValueBroadcast<LRBroadcast>*,bool> &collective = 
          release_region_trees.front();
        if (collective.second)
        {
          const LRBroadcast value = collective.first->get_value(false);
          runtime->forest->revoke_pending_region_tree(value.tid);
          runtime->free_distributed_id(value.did);
        }
        else
        {
          // Make sure this collective is done before we delete it
          const RtEvent done = collective.first->get_done_event();
          if (!done.has_triggered())
            done.wait();
        }
        delete collective.first;
        release_region_trees.pop_front();
      }
    }

    //--------------------------------------------------------------------------
    ApEvent ReplicateContext::add_to_dependence_queue(Operation *op,
                                                 bool unordered, bool outermost)
    //--------------------------------------------------------------------------
    {
      // We disable program order execution when we are replaying a
      // fixed trace since it might not be sound to block
      if (runtime->program_order_execution && !unordered &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
#ifdef DEBUG_LEGION
        assert(inorder_barrier.exists());
#endif
        ApEvent term_event = 
         InnerContext::add_to_dependence_queue(op,unordered,false/*outermost*/);
        Runtime::phase_barrier_arrive(inorder_barrier, 1/*count*/, term_event); 
        term_event = inorder_barrier;
        advance_replicate_barrier(inorder_barrier, total_shards);
        if (outermost)
        {
          begin_task_wait(true/*from runtime*/);
          term_event.wait();
          end_task_wait();
        }
        return term_event;
      }
      else
        return InnerContext::add_to_dependence_queue(op, unordered, outermost);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::record_dynamic_collective_contribution(
                                          DynamicCollective dc, const Future &f)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Illegal dynamic collective operation used in "
                    "control replicated task %s (UID %lld)", 
                    get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::find_collective_contributions(DynamicCollective dc,
                                             std::vector<Future> &contributions)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Illegal dynamic collective operation used in "
                    "control replicated task %s (UID %lld)",
                    get_task_name(), get_unique_id())
    } 

    //--------------------------------------------------------------------------
    ApBarrier ReplicateContext::create_phase_barrier(unsigned arrivals,
                                                     ReductionOpID redop,
                                                     const void *init_value,
                                                     size_t init_size)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && 
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_CREATE_PHASE_BARRIER);
        hasher.hash(arrivals);
        hasher.hash(redop);
        if (runtime->safe_control_replication > 1)
          hasher.hash(init_value, init_size);
        verify_replicable(hasher, "create_phase_barrier");
      }
      ValueBroadcast<ApBarrier> bar_collective(this, 0/*origin*/,
                                               COLLECTIVE_LOC_71); 
      // Shard 0 will make the barrier and broadcast it
      if (owner_shard->shard_id == 0)
      {
        ApBarrier result = InnerContext::create_phase_barrier(arrivals, redop,
                                                        init_value, init_size);
        bar_collective.broadcast(result);
        return result;
      }
      else
        return bar_collective.get_value();
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::destroy_phase_barrier(ApBarrier bar)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && 
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_DESTROY_PHASE_BARRIER);
        hasher.hash(bar);
        verify_replicable(hasher, "destroy_phase_barrier");
      }
      // Shard 0 has to wait for all the other shards to get here
      // too before it can do the deletion
      ShardSyncTree sync_point(this, 0/*origin*/, COLLECTIVE_LOC_72);
      if (owner_shard->shard_id == 0)
        InnerContext::destroy_phase_barrier(bar);
    }

    //--------------------------------------------------------------------------
    PhaseBarrier ReplicateContext::advance_phase_barrier(PhaseBarrier bar)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication &&
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ADVANCE_PHASE_BARRIER);
        hasher.hash(bar);
        verify_replicable(hasher, "advance_phase_barrier");
      }
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Advancing phase barrier in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
#endif
      PhaseBarrier result = bar;
      Runtime::advance_barrier(result);
#ifdef LEGION_SPY
      if (owner_shard->shard_id == 0)
        LegionSpy::log_event_dependence(bar.phase_barrier,result.phase_barrier);
#endif
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::arrive_dynamic_collective(DynamicCollective dc,
                                                    const void *buffer,
                                                    size_t size, unsigned count)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Illegal dynamic collective arrival performed in "
                    "control replicated task %s (UID %lld)",
                    get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::defer_dynamic_collective_arrival(
                                                         DynamicCollective dc,
                                                         const Future &f,
                                                         unsigned count)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Illegal defer dynamic collective arrival performed in "
                    "control replicated task %s (UID %lld)",
                    get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    Future ReplicateContext::get_dynamic_collective_result(DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_REPLICATE_TASK_VIOLATION,
                    "Illegal get dynamic collective result performed in "
                    "control replicated task %s (UID %lld)",
                    get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    DynamicCollective ReplicateContext::advance_dynamic_collective( 
                                                           DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (runtime->safe_control_replication && 
          ((current_trace == NULL) || !current_trace->is_fixed()))
      {
        Murmur3Hasher hasher;
        hasher.hash(REPLICATE_ADVANCE_DYNAMIC_COLLECTIVE);
        hasher.hash(dc);
        verify_replicable(hasher, "advance_dynamic_collective");
      }
#ifdef DEBUG_LEGION
      if (owner_shard->shard_id == 0)
        log_run.debug("Advancing dynamic collective in task %s (ID %lld)",
                        get_task_name(), get_unique_id());
#endif
      DynamicCollective result = dc;
      Runtime::advance_barrier(result);
#ifdef LEGION_SPY
      if (owner_shard->shard_id == 0)
        LegionSpy::log_event_dependence(dc.phase_barrier, result.phase_barrier);
#endif
      return result;
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    MergeCloseOp* ReplicateContext::get_merge_close_op(const LogicalUser &user,
                                                       RegionTreeNode *node)
#else
    MergeCloseOp* ReplicateContext::get_merge_close_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      ReplMergeCloseOp *result = runtime->get_available_repl_merge_close_op();
      // Get the mapped barrier for the close operation
      const unsigned close_index = next_close_mapped_bar_index++;
      if (next_close_mapped_bar_index == close_mapped_barriers.size())
        next_close_mapped_bar_index = 0;
      RtBarrier &mapped_bar = close_mapped_barriers[close_index];
#ifdef DEBUG_LEGION_COLLECTIVES
      CloseCheckReduction::RHS barrier(user, mapped_bar, 
                                       node, false/*read only*/);
      Runtime::phase_barrier_arrive(close_check_barrier, 1/*count*/,
                              RtEvent::NO_RT_EVENT, &barrier, sizeof(barrier));
      close_check_barrier.wait();
      CloseCheckReduction::RHS actual_barrier;
      bool ready = Runtime::get_barrier_result(close_check_barrier,
                                      &actual_barrier, sizeof(actual_barrier));
      assert(ready);
      assert(actual_barrier == barrier);
      advance_logical_barrier(close_check_barrier, total_shards);
#endif
      result->set_repl_close_info(mapped_bar);
      // Advance the phase for the next time through
      advance_logical_barrier(mapped_bar, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    RefinementOp* ReplicateContext::get_refinement_op(const LogicalUser &user,
                                                      RegionTreeNode *node)
#else
    RefinementOp* ReplicateContext::get_refinement_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      ReplRefinementOp *result = runtime->get_available_repl_refinement_op();
      // Get the mapped barrier for the refinement operation
      const unsigned refinement_index = next_refinement_mapped_bar_index++;
      if (next_refinement_mapped_bar_index == refinement_mapped_barriers.size())
        next_refinement_mapped_bar_index = 0;
      RtBarrier &mapped_bar = refinement_mapped_barriers[refinement_index];
#ifdef DEBUG_LEGION_COLLECTIVES
      CloseCheckReduction::RHS barrier(user, mapped_bar,
                                       node, false/*read only*/);
      Runtime::phase_barrier_arrive(refinement_check_barrier, 1/*count*/,
                              RtEvent::NO_RT_EVENT, &barrier, sizeof(barrier));
      refinement_check_barrier.wait();
      CloseCheckReduction::RHS actual_barrier;
      bool ready = Runtime::get_barrier_result(refinement_check_barrier,
                                      &actual_barrier, sizeof(actual_barrier));
      assert(ready);
      assert(actual_barrier == barrier);
      advance_logical_barrier(refinement_check_barrier, total_shards);
#endif
      const RtBarrier next_refinement_bar = get_next_refinement_barrier();
      result->set_repl_refinement_info(mapped_bar, next_refinement_bar);
      // Advance the phase for the next time through
      advance_logical_barrier(mapped_bar, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::pack_remote_context(Serializer &rez,
                                          AddressSpaceID target, bool replicate)
    //--------------------------------------------------------------------------
    {
      // Do the normal inner pack with replicate true
      InnerContext::pack_remote_context(rez, target, true/*replicate*/);
      // Then pack our additional information
      rez.serialize<size_t>(total_shards);
      rez.serialize(shard_manager->repl_id);
    }

    //--------------------------------------------------------------------------
    ShardingFunction* ReplicateContext::find_sharding_function(ShardingID sid)
    //--------------------------------------------------------------------------
    {
      return shard_manager->find_sharding_function(sid);
    }

    //--------------------------------------------------------------------------
    InstanceView* ReplicateContext::create_instance_top_view(
                                PhysicalManager *manager, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      // First do a little check to see if we already have it and if not record
      // that we're the first ones requesting from this replicate context
      RtEvent wait_on;
      bool send_request = false;
      {
        AutoLock inst_lock(instance_view_lock);
        std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
          instance_top_views.find(manager);
        if (finder != instance_top_views.end())
          return finder->second;
        // Didn't find it, see if we need to request it or whether 
        // someone else is already doing that
        std::map<PhysicalManager*,RtUserEvent>::const_iterator wait_finder = 
          pending_request_views.find(manager);
        if (wait_finder == pending_request_views.end())
        {
          RtUserEvent wait_for = Runtime::create_rt_user_event();
          pending_request_views[manager] = wait_for;
          wait_on = wait_for;
          send_request = true;
        }
        else
          wait_on = wait_finder->second;
      }
      // Send the request if we're first
      // Since we are a control replicated context we have to bounce this
      // off the shard manager to find the right context to make the view
      if (send_request)
        shard_manager->create_instance_top_view(manager, source, this,
                                                runtime->address_space);
      // Wait for the result to be ready
      wait_on.wait();
      // Retake the lock and retrieve the result
      AutoLock inst_lock(instance_view_lock,1,false/*exclusive*/);
#ifdef DEBUG_LEGION
      assert(instance_top_views.find(manager) != instance_top_views.end());
#endif
      return instance_top_views[manager];
    }

    //--------------------------------------------------------------------------
    InstanceView* ReplicateContext::create_replicate_instance_top_view(
                                PhysicalManager *manager, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      // If we got picked then we can just do the base inner version
      return InnerContext::create_instance_top_view(manager, source);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::record_replicate_instance_top_view(
                                 PhysicalManager *manager, InstanceView *result)
    //--------------------------------------------------------------------------
    {
      // Always add the reference, we'll remove duplicates if necessary
      result->add_base_resource_ref(CONTEXT_REF);
      bool remove_duplicate_reference = false;
      RtUserEvent to_trigger;
      {
        AutoLock inst_lock(instance_view_lock);
        std::map<PhysicalManager*,InstanceView*>::const_iterator finder = 
          instance_top_views.find(manager);
        if (finder != instance_top_views.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second == result);
#endif
          remove_duplicate_reference = true;
        }
        else
          instance_top_views[manager] = result;
        // Now we find the event to trigger
        std::map<PhysicalManager*,RtUserEvent>::iterator pending_finder = 
          pending_request_views.find(manager);
#ifdef DEBUG_LEGION
        assert(pending_finder != pending_request_views.end());
#endif
        to_trigger = pending_finder->second;
        pending_request_views.erase(pending_finder);
      }
      Runtime::trigger_event(to_trigger);
      if (remove_duplicate_reference && 
          result->remove_base_resource_ref(CONTEXT_REF))
        delete result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::exchange_common_resources(void)
    //--------------------------------------------------------------------------
    {
      size_t num_barriers = LEGION_CONTROL_REPLICATION_COMMUNICATION_BARRIERS;
      if (shard_manager->total_shards > num_barriers)
        num_barriers = shard_manager->total_shards;
      // Exchange close map barriers across all the shards
      BarrierExchangeCollective<RtBarrier> mapped_collective(this,
          num_barriers, close_mapped_barriers, COLLECTIVE_LOC_50);
      mapped_collective.exchange_barriers_async();
      BarrierExchangeCollective<RtBarrier> refinement_ready_collective(this,
          num_barriers, refinement_ready_barriers, COLLECTIVE_LOC_23);
      refinement_ready_collective.exchange_barriers_async();
      BarrierExchangeCollective<RtBarrier> refinement_collective(this,
          num_barriers, refinement_mapped_barriers, COLLECTIVE_LOC_19);
      refinement_collective.exchange_barriers_async();
      BarrierExchangeCollective<ApBarrier> indirect_collective(this,
          num_barriers, indirection_barriers, COLLECTIVE_LOC_79);
      indirect_collective.exchange_barriers_async();
      BarrierExchangeCollective<RtBarrier> future_map_collective(this,
          num_barriers, future_map_barriers, COLLECTIVE_LOC_90);
      future_map_collective.exchange_barriers_async();
      // Wait for everything to be done
      mapped_collective.wait_for_barrier_exchange();
      refinement_ready_collective.wait_for_barrier_exchange();
      refinement_collective.wait_for_barrier_exchange();
      indirect_collective.wait_for_barrier_exchange();
      future_map_collective.wait_for_barrier_exchange();
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_collective_message(Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      ShardCollective *collective = find_or_buffer_collective(derez);   
      if (collective != NULL)
        collective->handle_collective_message(derez);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_future_map_request(Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      ReplFutureMapImpl *impl = find_or_buffer_future_map_request(derez);
      // If impl is NULL then the request was buffered
      if (impl == NULL)
        return;
      impl->handle_future_map_request(derez);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_disjoint_complete_request(Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      LogicalRegion handle;
      derez.deserialize(handle);
      VersionManager *target;
      derez.deserialize(target);
      AddressSpaceID target_space;
      derez.deserialize(target_space);
      FieldMask request_mask;
      derez.deserialize(request_mask);
      UniqueID opid;
      derez.deserialize(opid);
      AddressSpaceID original_source;
      derez.deserialize(original_source);
      RtUserEvent done_event;
      derez.deserialize(done_event);

      RegionNode *node = runtime->forest->get_node(handle);
      VersionInfo *result_info = new VersionInfo();
      std::set<RtEvent> ready_events;
      node->perform_versioning_analysis(tree_context.get_id(), this,
          result_info, request_mask, opid, original_source, ready_events);
      // In general these ready events should be empty because we 
      // are on the shard that owns these eqivalence sets so it should
      // just be able to compute them right away. We wait though just
      // in case it is necessary, but it should be rare.
      if (!ready_events.empty())
      {
        const RtEvent ready = Runtime::merge_events(ready_events);
        if (ready.exists() && !ready.has_triggered())
        {
          DeferDisjointCompleteResponseArgs args(opid, target,
                        target_space, result_info, done_event);
          runtime->issue_runtime_meta_task(args, 
              LG_LATENCY_DEFERRED_PRIORITY, ready);
          return;
        }
      }
      finalize_disjoint_complete_response(runtime, opid, target, target_space,
                                          result_info, done_event);
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::handle_disjoint_complete_response(
                                          Deserializer &derez, Runtime *runtime)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      VersionManager *target;
      derez.deserialize(target);
      size_t num_sets;
      derez.deserialize(num_sets);
      VersionInfo *version_info = new VersionInfo();
      std::set<RtEvent> ready_events;
      for (unsigned idx = 0; idx < num_sets; idx++)
      {
        DistributedID did;
        derez.deserialize(did);
        RtEvent ready_event;
        EquivalenceSet *set = 
          runtime->find_or_request_equivalence_set(did, ready_event);
        if (ready_event.exists())
          ready_events.insert(ready_event);
        FieldMask mask;
        derez.deserialize(mask);
        version_info->record_equivalence_set(set, mask);
      }
      UniqueID opid;
      derez.deserialize(opid);
      RtUserEvent done_event;
      derez.deserialize(done_event);
      if (!ready_events.empty())
      {
        const RtEvent ready = Runtime::merge_events(ready_events);
        if (ready.exists() && !ready.has_triggered())
        {
          DeferDisjointCompleteResponseArgs args(opid, target, 
              runtime->address_space, version_info, done_event);
          runtime->issue_runtime_meta_task(args, 
              LG_LATENCY_DEFERRED_PRIORITY, ready);
          return;
        }
      }
      finalize_disjoint_complete_response(runtime, opid, target, 
              runtime->address_space, version_info, done_event);
    }

    //--------------------------------------------------------------------------
    ReplicateContext::DeferDisjointCompleteResponseArgs::
      DeferDisjointCompleteResponseArgs(UniqueID opid, VersionManager *t,
                      AddressSpaceID s, VersionInfo *info, RtUserEvent d)
      : LgTaskArgs<DeferDisjointCompleteResponseArgs>(opid), target(t),
        target_space(s), version_info(info), done_event(d)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::handle_defer_disjoint_complete_response(
                                             Runtime *runtime, const void *args)
    //--------------------------------------------------------------------------
    {
      const DeferDisjointCompleteResponseArgs *dargs =
        (const DeferDisjointCompleteResponseArgs*)args;
      finalize_disjoint_complete_response(runtime, dargs->provenance, 
       dargs->target,dargs->target_space,dargs->version_info,dargs->done_event);
    }

    //--------------------------------------------------------------------------
    /*static*/ void ReplicateContext::finalize_disjoint_complete_response(
        Runtime *runtime, UniqueID opid, VersionManager *target,
        AddressSpaceID target_space, VersionInfo *info, RtUserEvent done_event)
    //--------------------------------------------------------------------------
    {
      const FieldMaskSet<EquivalenceSet> &result_sets =
        info->get_equivalence_sets();
      if (target_space == runtime->address_space)
      {
        // local case
        FieldMask dummy_parent;
        std::set<RtEvent> applied_events;
        for (FieldMaskSet<EquivalenceSet>::const_iterator it =
              result_sets.begin(); it != result_sets.end(); it++)
          target->record_refinement(it->first, it->second, 
                                    dummy_parent, applied_events);
        if (!applied_events.empty())
          Runtime::trigger_event(done_event, 
              Runtime::merge_events(applied_events));
        else
          Runtime::trigger_event(done_event);
      }
      else
      {
        // remote case: send back to the owner
        Serializer rez;
        {
          RezCheck z(rez);
          rez.serialize(target);
          rez.serialize<size_t>(result_sets.size());
          for (FieldMaskSet<EquivalenceSet>::const_iterator it =
              result_sets.begin(); it != result_sets.end(); it++)
          {
            rez.serialize(it->first->did);
            rez.serialize(it->second);
          }
          rez.serialize(opid);
          rez.serialize(done_event);
        }
        runtime->send_control_replicate_disjoint_complete_response(target_space,
                                                                   rez);
      }
      // always delete the version info
      delete info;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_resource_update(Deserializer &derez,
                                                  std::set<RtEvent> &applied)
    //--------------------------------------------------------------------------
    {
      size_t return_index;
      derez.deserialize(return_index);
      RtBarrier ready_barrier, mapped_barrier, execution_barrier;
      derez.deserialize(ready_barrier);
      derez.deserialize(mapped_barrier);
      derez.deserialize(execution_barrier);
      size_t num_created_regions;
      derez.deserialize(num_created_regions);
      std::map<LogicalRegion,unsigned> created_regs;
      for (unsigned idx = 0; idx < num_created_regions; idx++)
      {
        LogicalRegion reg;
        derez.deserialize(reg);
        derez.deserialize(created_regs[reg]);
      }
      size_t num_deleted_regions;
      derez.deserialize(num_deleted_regions);
      std::vector<LogicalRegion> deleted_regs(num_deleted_regions);
      for (unsigned idx = 0; idx < num_deleted_regions; idx++)
        derez.deserialize(deleted_regs[idx]);
      size_t num_created_fields;
      derez.deserialize(num_created_fields);
      std::set<std::pair<FieldSpace,FieldID> > created_fids;
      for (unsigned idx = 0; idx < num_created_fields; idx++)
      {
        std::pair<FieldSpace,FieldID> key;
        derez.deserialize(key.first);
        derez.deserialize(key.second);
        created_fids.insert(key);
      }
      size_t num_deleted_fields;
      derez.deserialize(num_deleted_fields);
      std::vector<std::pair<FieldSpace,FieldID> > 
          deleted_fids(num_deleted_fields);
      for (unsigned idx = 0; idx < num_deleted_fields; idx++)
      {
        derez.deserialize(deleted_fids[idx].first);
        derez.deserialize(deleted_fids[idx].second);
      }
      size_t num_created_field_spaces;
      derez.deserialize(num_created_field_spaces);
      std::map<FieldSpace,unsigned> created_fs;
      for (unsigned idx = 0; idx < num_created_field_spaces; idx++)
      {
        FieldSpace sp;
        derez.deserialize(sp);
        derez.deserialize(created_fs[sp]);
      }
      size_t num_latent_field_spaces;
      derez.deserialize(num_latent_field_spaces);
      std::map<FieldSpace,std::set<LogicalRegion> > latent_fs;
      for (unsigned idx = 0; idx < num_latent_field_spaces; idx++)
      {
        FieldSpace sp;
        derez.deserialize(sp);
        std::set<LogicalRegion> &regions = latent_fs[sp];
        size_t num_regions;
        derez.deserialize(num_regions);
        for (unsigned idx2 = 0; idx2 < num_regions; idx2++)
        {
          LogicalRegion region;
          derez.deserialize(region);
          regions.insert(region);
        }
      }
      size_t num_deleted_field_spaces;
      derez.deserialize(num_deleted_field_spaces);
      std::vector<FieldSpace> deleted_fs(num_deleted_field_spaces);
      for (unsigned idx = 0; idx < num_deleted_field_spaces; idx++)
        derez.deserialize(deleted_fs[idx]);
      size_t num_created_index_spaces;
      derez.deserialize(num_created_index_spaces);
      std::map<IndexSpace,unsigned> created_is;
      for (unsigned idx = 0; idx < num_created_index_spaces; idx++)
      {
        IndexSpace sp;
        derez.deserialize(sp);
        derez.deserialize(created_is[sp]);
      }
      size_t num_deleted_index_spaces;
      derez.deserialize(num_deleted_index_spaces);
      std::vector<std::pair<IndexSpace,bool> > 
          deleted_is(num_deleted_index_spaces);
      for (unsigned idx = 0; idx < num_deleted_index_spaces; idx++)
      {
        derez.deserialize(deleted_is[idx].first);
        derez.deserialize(deleted_is[idx].second);
      }
      size_t num_created_index_partitions;
      derez.deserialize(num_created_index_partitions);
      std::map<IndexPartition,unsigned> created_partitions;
      for (unsigned idx = 0; idx < num_created_index_partitions; idx++)
      {
        IndexPartition ip;
        derez.deserialize(ip);
        derez.deserialize(created_partitions[ip]);
      }
      size_t num_deleted_index_partitions;
      derez.deserialize(num_deleted_index_partitions);
      std::vector<std::pair<IndexPartition,bool> > 
          deleted_partitions(num_deleted_index_partitions);
      for (unsigned idx = 0; idx < num_deleted_index_partitions; idx++)
      {
        derez.deserialize(deleted_partitions[idx].first);
        derez.deserialize(deleted_partitions[idx].second);
      }
      // Send this down to the base class to avoid re-broadcasting
      receive_replicate_resources(return_index, created_regs, deleted_regs,
          created_fids, deleted_fids, created_fs, latent_fs, deleted_fs,
          created_is, deleted_is, created_partitions, deleted_partitions,
          applied, ready_barrier, mapped_barrier, execution_barrier);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_created_region_contexts(Deserializer &derez,
                                              std::set<RtEvent> &applied_events)
    //--------------------------------------------------------------------------
    {
      AddressSpaceID source;
      derez.deserialize(source);
      size_t num_shards;
      derez.deserialize(num_shards);
      size_t num_regions;
      derez.deserialize(num_regions);
      std::vector<RegionNode*> created_states(num_regions);
      const RegionTreeContext ctx = runtime->allocate_region_tree_context();
      for (unsigned idx = 0; idx < num_regions; idx++)
      {
        LogicalRegion handle;
        derez.deserialize(handle);
        RegionNode *node = runtime->forest->get_node(handle);
        node->unpack_logical_state(ctx.get_id(), derez, source);
        node->unpack_version_state(ctx.get_id(), derez, source);
        created_states[idx] = node;
      }
      receive_replicate_created_region_contexts(ctx, created_states,
                                  applied_events, num_shards, NULL);
      runtime->free_region_tree_context(ctx);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_trace_update(Deserializer &derez, 
                                               AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      ShardedPhysicalTemplate *tpl = find_or_buffer_trace_update(derez, source);
      // If the template is NULL then the request was buffered
      if (tpl == NULL)
        return;
      tpl->handle_trace_update(derez, source);
    }

    //--------------------------------------------------------------------------
    ApBarrier ReplicateContext::handle_find_trace_shard_event(
                     size_t template_index, ApEvent event, ShardID remote_shard)
    //--------------------------------------------------------------------------
    {
      ShardedPhysicalTemplate *physical_template = NULL;
      {
        AutoLock r_lock(replication_lock);
        std::map<size_t,ShardedPhysicalTemplate*>::const_iterator finder = 
          physical_templates.find(template_index);
        // If we can't find the template index that means it hasn't been
        // started here so it can't have produced the event we're looking for
        // Note it also can't have been reclaimed yet as all the shard
        // templates need to come to the same decision on whether they 
        // are replayable before any of them can be deleted and so if one
        // is still tracing then they all are
        if (finder == physical_templates.end())
          return ApBarrier::NO_AP_BARRIER;
        physical_template = finder->second;
      }
      return physical_template->find_trace_shard_event(event, remote_shard);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::record_intra_space_dependence(size_t context_index,
        const DomainPoint &point, RtEvent point_mapped, ShardID next_shard)
    //--------------------------------------------------------------------------
    {
      const std::pair<size_t,DomainPoint> key(context_index,point);
      AutoLock r_lock(replication_lock);
      IntraSpaceDeps &deps = intra_space_deps[key];
      // Check to see if someone has already registered this
      std::map<ShardID,RtUserEvent>::iterator finder = 
        deps.pending_deps.find(next_shard);
      if (finder != deps.pending_deps.end())
      {
        Runtime::trigger_event(finder->second, point_mapped);
        deps.pending_deps.erase(finder);
        if (deps.pending_deps.empty() && deps.ready_deps.empty())
          intra_space_deps.erase(key);
      }
      else
      {
        // Not seen yet so just record our entry for this shard
#ifdef DEBUG_LEGION
        assert(deps.ready_deps.find(next_shard) == deps.ready_deps.end());
#endif
        deps.ready_deps[next_shard] = point_mapped;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::handle_intra_space_dependence(Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      std::pair<size_t,DomainPoint> key;
      derez.deserialize(key.first);
      derez.deserialize(key.second);
      RtUserEvent pending_event;
      derez.deserialize(pending_event);
      ShardID requesting_shard;
      derez.deserialize(requesting_shard);

      AutoLock r_lock(replication_lock);
      IntraSpaceDeps &deps = intra_space_deps[key];
      // Check to see if someone has already registered this shard
      std::map<ShardID,RtEvent>::iterator finder = 
        deps.ready_deps.find(requesting_shard);
      if (finder != deps.ready_deps.end())
      {
        Runtime::trigger_event(pending_event, finder->second);
        deps.ready_deps.erase(finder);
        if (deps.ready_deps.empty() && deps.pending_deps.empty())
          intra_space_deps.erase(key);
      }
      else
      {
        // Not seen yet so just record our entry for this shard
#ifdef DEBUG_LEGION
        assert(deps.pending_deps.find(requesting_shard) == 
                deps.pending_deps.end());
#endif
        deps.pending_deps[requesting_shard] = pending_event;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::receive_resources(size_t return_index,
              std::map<LogicalRegion,unsigned> &created_regs,
              std::vector<LogicalRegion> &deleted_regs,
              std::set<std::pair<FieldSpace,FieldID> > &created_fids,
              std::vector<std::pair<FieldSpace,FieldID> > &deleted_fids,
              std::map<FieldSpace,unsigned> &created_fs,
              std::map<FieldSpace,std::set<LogicalRegion> > &latent_fs,
              std::vector<FieldSpace> &deleted_fs,
              std::map<IndexSpace,unsigned> &created_is,
              std::vector<std::pair<IndexSpace,bool> > &deleted_is,
              std::map<IndexPartition,unsigned> &created_partitions,
              std::vector<std::pair<IndexPartition,bool> > &deleted_partitions,
              std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      // We need to broadcast these updates out to other shards
      Serializer rez;
      // If we have any deletions make barriers for use with
      // the deletion operations we may need to perform
      if (!deleted_regs.empty() || !deleted_fids.empty() || 
          !deleted_fs.empty() || !deleted_is.empty() || 
          !deleted_partitions.empty())
      {
        if (!returned_resource_ready_barrier.exists())
          returned_resource_ready_barrier = RtBarrier(
              Realm::Barrier::create_barrier(shard_manager->total_shards));
        if (!returned_resource_mapped_barrier.exists())
          returned_resource_mapped_barrier = RtBarrier(
              Realm::Barrier::create_barrier(shard_manager->total_shards));
        if (!returned_resource_execution_barrier.exists())
          returned_resource_execution_barrier = RtBarrier(
              Realm::Barrier::create_barrier(shard_manager->total_shards));
      }
      rez.serialize(return_index);
      rez.serialize(returned_resource_ready_barrier);
      rez.serialize(returned_resource_mapped_barrier);
      rez.serialize(returned_resource_execution_barrier);
      rez.serialize<size_t>(created_regs.size());
      if (!created_regs.empty())
      {
        for (std::map<LogicalRegion,unsigned>::const_iterator it =
              created_regs.begin(); it != created_regs.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(deleted_regs.size());
      if (!deleted_regs.empty())
      {
        for (std::vector<LogicalRegion>::const_iterator it = 
              deleted_regs.begin(); it != deleted_regs.end(); it++)
          rez.serialize(*it);
      }
      rez.serialize<size_t>(created_fids.size());
      if (!created_fids.empty())
      {
        for (std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
              it = created_fids.begin(); it != created_fids.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(deleted_fids.size());
      if (!deleted_fids.empty())
      {
        for (std::vector<std::pair<FieldSpace,FieldID> >::const_iterator it =
              deleted_fids.begin(); it != deleted_fids.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(created_fs.size());
      if (!created_fs.empty())
      {
        for (std::map<FieldSpace,unsigned>::const_iterator it = 
              created_fs.begin(); it != created_fs.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      } 
      rez.serialize<size_t>(latent_fs.size());
      if (!latent_fs.empty())
      {
        for (std::map<FieldSpace,std::set<LogicalRegion> >::const_iterator it =
              latent_fs.begin(); it != latent_fs.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize<size_t>(it->second.size());
          for (std::set<LogicalRegion>::const_iterator it2 = 
                it->second.begin(); it2 != it->second.end(); it2++)
            rez.serialize(*it2);
        }
      }
      rez.serialize<size_t>(deleted_fs.size());
      if (!deleted_fs.empty())
      {
        for (std::vector<FieldSpace>::const_iterator it = 
              deleted_fs.begin(); it != deleted_fs.end(); it++)
          rez.serialize(*it);
      }
      rez.serialize<size_t>(created_is.size());
      if (!created_is.empty())
      {
        for (std::map<IndexSpace,unsigned>::const_iterator it = 
              created_is.begin(); it != created_is.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(deleted_is.size());
      if (!deleted_is.empty())
      {
        for (std::vector<std::pair<IndexSpace,bool> >::const_iterator it = 
              deleted_is.begin(); it != deleted_is.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(created_partitions.size());
      if (!created_partitions.empty())
      {
        for (std::map<IndexPartition,unsigned>::const_iterator it = 
              created_partitions.begin(); it != 
              created_partitions.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      rez.serialize<size_t>(deleted_partitions.size());
      if (!deleted_partitions.empty())
      {
        for (std::vector<std::pair<IndexPartition,bool> >::const_iterator it = 
              deleted_partitions.begin(); it != deleted_partitions.end(); it++)
        {
          rez.serialize(it->first);
          rez.serialize(it->second);
        }
      }
      shard_manager->broadcast_resource_update(owner_shard, rez, preconditions);
      // Now we can handle this for ourselves
      receive_replicate_resources(return_index, created_regs, deleted_regs,
          created_fids, deleted_fids, created_fs, latent_fs, deleted_fs,
          created_is, deleted_is, created_partitions, deleted_partitions,
          preconditions, returned_resource_ready_barrier,
          returned_resource_mapped_barrier,returned_resource_execution_barrier);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::receive_replicate_resources(size_t return_index,
              std::map<LogicalRegion,unsigned> &created_regs,
              std::vector<LogicalRegion> &deleted_regs,
              std::set<std::pair<FieldSpace,FieldID> > &created_fids,
              std::vector<std::pair<FieldSpace,FieldID> > &deleted_fids,
              std::map<FieldSpace,unsigned> &created_fs,
              std::map<FieldSpace,std::set<LogicalRegion> > &latent_fs,
              std::vector<FieldSpace> &deleted_fs,
              std::map<IndexSpace,unsigned> &created_is,
              std::vector<std::pair<IndexSpace,bool> > &deleted_is,
              std::map<IndexPartition,unsigned> &created_partitions,
              std::vector<std::pair<IndexPartition,bool> > &deleted_partitions,
              std::set<RtEvent> &preconditions, RtBarrier &ready_barrier, 
              RtBarrier &mapped_barrier, RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      bool need_deletion_dependences = true;
      ApEvent precondition;
      std::map<Operation*,GenerationID> dependences;
      if (!created_regs.empty())
        register_region_creations(created_regs);
      if (!deleted_regs.empty())
      {
        precondition = 
          compute_return_deletion_dependences(return_index, dependences);
        need_deletion_dependences = false;
        register_region_deletions(precondition, dependences, 
                                  deleted_regs, preconditions, ready_barrier,
                                  mapped_barrier, execution_barrier); 
      }
      if (!created_fids.empty())
        register_field_creations(created_fids);
      if (!deleted_fids.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_field_deletions(precondition, dependences, 
                                 deleted_fids, preconditions, ready_barrier,
                                 mapped_barrier, execution_barrier);
      }
      if (!created_fs.empty())
        register_field_space_creations(created_fs);
      if (!latent_fs.empty())
        register_latent_field_spaces(latent_fs);
      if (!deleted_fs.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_field_space_deletions(precondition, dependences,
                                       deleted_fs, preconditions, ready_barrier,
                                       mapped_barrier, execution_barrier);
      }
      if (!created_is.empty())
        register_index_space_creations(created_is);
      if (!deleted_is.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_index_space_deletions(precondition, dependences,
                                       deleted_is, preconditions, ready_barrier,
                                       mapped_barrier, execution_barrier);
      }
      if (!created_partitions.empty())
        register_index_partition_creations(created_partitions);
      if (!deleted_partitions.empty())
      {
        if (need_deletion_dependences)
        {
          precondition = 
            compute_return_deletion_dependences(return_index, dependences);
          need_deletion_dependences = false;
        }
        register_index_partition_deletions(precondition, dependences,
                                           deleted_partitions, preconditions,
                                           ready_barrier, mapped_barrier, 
                                           execution_barrier);
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_region_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                                            std::vector<LogicalRegion> &regions,
                                            std::set<RtEvent> &preconditions,
                                            RtBarrier &ready_barrier,
                                            RtBarrier &mapped_barrier,
                                            RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      std::vector<LogicalRegion> delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<LogicalRegion>::const_iterator rit =
              regions.begin(); rit != regions.end(); rit++)
        {
          std::map<LogicalRegion,unsigned>::iterator region_finder = 
            created_regions.find(*rit);
          if (region_finder == created_regions.end())
          {
            if (local_regions.find(*rit) != local_regions.end())
              REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
                  "Local logical region (%x,%x,%x) in task %s (UID %lld) was "
                  "not deleted by this task. Local regions can only be deleted "
                  "by the task that made them.", rit->index_space.id,
                  rit->field_space.id, rit->tree_id, 
                  get_task_name(), get_unique_id())
            // Deletion keeps going up
            deleted_regions.push_back(*rit);
          }
          else
          {
            // One of ours to delete
#ifdef DEBUG_LEGION
            assert(region_finder->second > 0);
#endif
            if (--region_finder->second == 0)
            {
              // Don't remove this from created regions yet,
              // That will happen when we make the deletion operation
              delete_now.push_back(*rit);
              // Check to see if we have any latent field spaces to clean up
              if (!latent_field_spaces.empty())
              {
                std::map<FieldSpace,std::set<LogicalRegion> >::iterator finder =
                  latent_field_spaces.find(rit->get_field_space());
                if (finder != latent_field_spaces.end())
                {
                  std::set<LogicalRegion>::iterator latent_finder =
                    finder->second.find(*rit);
#ifdef DEBUG_LEGION
                  assert(latent_finder != finder->second.end());
#endif
                  finder->second.erase(latent_finder);
                  if (finder->second.empty())
                  {
                    // Now that all the regions using this field space have
                    // been deleted we can clean up all the created_fields
                    for (std::set<std::pair<FieldSpace,FieldID> >::iterator it =
                          created_fields.begin(); it !=
                          created_fields.end(); /*nothing*/)
                    {
                      if (it->first == finder->first)
                      {
                        std::set<std::pair<FieldSpace,FieldID> >::iterator
                          to_delete = it++;
                        created_fields.erase(to_delete);
                      }
                      else
                        it++;
                    }
                    latent_field_spaces.erase(finder);
                  }
                }
              }
            }
          }
        }
      }
      if (!delete_now.empty())
      {
        for (std::vector<LogicalRegion>::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
          op->initialize_logical_region_deletion(this, *it, true/*unordered*/,
                                            true/*skip dependence analysis*/);
          op->initialize_replication(this, ready_barrier, mapped_barrier, 
              execution_barrier, shard_manager->is_total_sharding(),
              shard_manager->is_first_local_shard(owner_shard));
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_field_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                           std::vector<std::pair<FieldSpace,FieldID> > &fields,
                           std::set<RtEvent> &preconditions,
                           RtBarrier &ready_barrier, RtBarrier &mapped_barrier,
                           RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      std::map<FieldSpace,std::set<FieldID> > delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<FieldSpace,FieldID> >::const_iterator fit =
              fields.begin(); fit != fields.end(); fit++)
        {
          std::set<std::pair<FieldSpace,FieldID> >::const_iterator 
            field_finder = created_fields.find(*fit);
          if (field_finder == created_fields.end())
          {
            std::map<std::pair<FieldSpace,FieldID>,bool>::iterator 
              local_finder = local_fields.find(*fit);
            if (local_finder != local_fields.end())
              REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
                  "Local field %d in field space %x in task %s (UID %lld) was "
                  "not deleted by this task. Local fields can only be deleted "
                  "by the task that made them.", fit->second, fit->first.id,
                  get_task_name(), get_unique_id())
            deleted_fields.push_back(*fit);
          }
          else
          {
            // One of ours to delete
            delete_now[fit->first].insert(fit->second);
            created_fields.erase(field_finder);
          }
        }
      }
      if (!delete_now.empty())
      {
        for (std::map<FieldSpace,std::set<FieldID> >::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
          FieldAllocatorImpl *allocator = 
            create_field_allocator(it->first, true/*unordered*/);
          op->initialize_field_deletions(this, it->first, it->second, 
              true/*unordered*/, allocator, (owner_shard->shard_id != 0),
              true/*skip dependence analysis*/);
          op->initialize_replication(this, ready_barrier, mapped_barrier, 
              execution_barrier, shard_manager->is_total_sharding(),
              shard_manager->is_first_local_shard(owner_shard));
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_field_space_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                                               std::vector<FieldSpace> &spaces,
                                               std::set<RtEvent> &preconditions,
                                               RtBarrier &ready_barrier,
                                               RtBarrier &mapped_barrier,
                                               RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      std::vector<FieldSpace> delete_now;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<FieldSpace>::const_iterator fit = 
              spaces.begin(); fit != spaces.end(); fit++)
        {
          std::map<FieldSpace,unsigned>::iterator finder = 
            created_field_spaces.find(*fit);
          if (finder != created_field_spaces.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(*fit);
              created_field_spaces.erase(finder);
              // Count how many regions are still using this field space
              // that still need to be deleted before we can remove the
              // list of created fields
              std::set<LogicalRegion> remaining_regions;
              for (std::map<LogicalRegion,unsigned>::const_iterator it = 
                    created_regions.begin(); it != created_regions.end(); it++)
                if (it->first.get_field_space() == *fit)
                  remaining_regions.insert(it->first);
              for (std::map<LogicalRegion,bool>::const_iterator it = 
                    local_regions.begin(); it != local_regions.end(); it++)
                if (it->first.get_field_space() == *fit)
                  remaining_regions.insert(it->first);
              if (remaining_regions.empty())
              {
                // No remaining regions so we can remove any created fields now
                for (std::set<std::pair<FieldSpace,FieldID> >::iterator it = 
                      created_fields.begin(); it != 
                      created_fields.end(); /*nothing*/)
                {
                  if (it->first == *fit)
                  {
                    std::set<std::pair<FieldSpace,FieldID> >::iterator 
                      to_delete = it++;
                    created_fields.erase(to_delete);
                  }
                  else
                    it++;
                }
              }
              else
                latent_field_spaces[*fit] = remaining_regions;
            }
          }
          else
            // If we didn't make this field space, record the deletion
            // and keep going. It will be handled by the context that
            // made the field space
            deleted_field_spaces.push_back(*fit);
        }
      }
      if (!delete_now.empty())
      {
        for (std::vector<FieldSpace>::const_iterator it = 
              delete_now.begin(); it != delete_now.end(); it++)
        {
          ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
          op->initialize_field_space_deletion(this, *it, true/*unordered*/);
          op->initialize_replication(this, ready_barrier, mapped_barrier, 
              execution_barrier, shard_manager->is_total_sharding(),
              shard_manager->is_first_local_shard(owner_shard));
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_index_space_deletions(ApEvent precondition,
                           const std::map<Operation*,GenerationID> &dependences,
                               std::vector<std::pair<IndexSpace,bool> > &spaces,
                                               std::set<RtEvent> &preconditions,
                                               RtBarrier &ready_barrier,
                                               RtBarrier &mapped_barrier,
                                               RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      std::vector<IndexSpace> delete_now;
      std::vector<std::vector<IndexPartition> > sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<IndexSpace,bool> >::const_iterator sit =
              spaces.begin(); sit != spaces.end(); sit++)
        {
          std::map<IndexSpace,unsigned>::iterator finder = 
            created_index_spaces.find(sit->first);
          if (finder != created_index_spaces.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(sit->first);
              sub_partitions.resize(sub_partitions.size() + 1);
              created_index_spaces.erase(finder);
              if (sit->second)
              {
                std::vector<IndexPartition> &subs = sub_partitions.back();
                // Also remove any index partitions for this index space tree
                for (std::map<IndexPartition,unsigned>::iterator it = 
                      created_index_partitions.begin(); it !=
                      created_index_partitions.end(); /*nothing*/)
                {
                  if (it->first.get_tree_id() == sit->first.get_tree_id()) 
                  {
#ifdef DEBUG_LEGION
                    assert(it->second > 0);
#endif
                    if (--it->second == 0)
                    {
                      subs.push_back(it->first);
                      std::map<IndexPartition,unsigned>::iterator 
                        to_delete = it++;
                      created_index_partitions.erase(to_delete);
                    }
                    else
                      it++;
                  }
                  else
                    it++;
                }
              }
            }
          }
          else
            // If we didn't make the index space in this context, just
            // record it and keep going, it will get handled later
            deleted_index_spaces.push_back(*sit);
        }
      }
      if (!delete_now.empty())
      {
#ifdef DEBUG_LEGION
        assert(delete_now.size() == sub_partitions.size());
#endif
        for (unsigned idx = 0; idx < delete_now.size(); idx++)
        {
          ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
          op->initialize_index_space_deletion(this, delete_now[idx], 
                            sub_partitions[idx], true/*unordered*/);
          op->initialize_replication(this, ready_barrier, mapped_barrier, 
              execution_barrier, shard_manager->is_total_sharding(),
              shard_manager->is_first_local_shard(owner_shard));
          op->set_execution_precondition(precondition);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_index_partition_deletions(ApEvent precond,
                           const std::map<Operation*,GenerationID> &dependences,
                            std::vector<std::pair<IndexPartition,bool> > &parts, 
                                               std::set<RtEvent> &preconditions,
                                               RtBarrier &ready_barrier,
                                               RtBarrier &mapped_barrier,
                                               RtBarrier &execution_barrier)
    //--------------------------------------------------------------------------
    {
      std::vector<IndexPartition> delete_now;
      std::vector<std::vector<IndexPartition> > sub_partitions;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::vector<std::pair<IndexPartition,bool> >::const_iterator pit =
              parts.begin(); pit != parts.end(); pit++)
        {
          std::map<IndexPartition,unsigned>::iterator finder = 
            created_index_partitions.find(pit->first);
          if (finder != created_index_partitions.end())
          {
#ifdef DEBUG_LEGION
            assert(finder->second > 0);
#endif
            if (--finder->second == 0)
            {
              delete_now.push_back(pit->first);
              sub_partitions.resize(sub_partitions.size() + 1);
              created_index_partitions.erase(finder);
              if (pit->second)
              {
                std::vector<IndexPartition> &subs = sub_partitions.back();
                // Remove any other partitions that this partition dominates
                for (std::map<IndexPartition,unsigned>::iterator it = 
                      created_index_partitions.begin(); it !=
                      created_index_partitions.end(); /*nothing*/)
                {
                  if ((pit->first.get_tree_id() == it->first.get_tree_id()) &&
                      runtime->forest->is_dominated_tree_only(it->first, 
                                                              pit->first))
                  {
#ifdef DEBUG_LEGION
                    assert(it->second > 0);
#endif
                    if (--it->second == 0)
                    {
                      subs.push_back(it->first);
                      std::map<IndexPartition,unsigned>::iterator 
                        to_delete = it++;
                      created_index_partitions.erase(to_delete);
                    }
                    else
                      it++;
                  }
                  else
                    it++;
                }
              }
            }
          }
          else
            // If we didn't make the partition, record it and keep going
            deleted_index_partitions.push_back(*pit);
        }
      }
      if (!delete_now.empty())
      {
#ifdef DEBUG_LEGION
        assert(delete_now.size() == sub_partitions.size());
#endif
        for (unsigned idx = 0; idx < delete_now.size(); idx++)
        {
          ReplDeletionOp *op = runtime->get_available_repl_deletion_op();
          op->initialize_index_part_deletion(this, delete_now[idx], 
                            sub_partitions[idx], true/*unordered*/);
          op->initialize_replication(this, ready_barrier, mapped_barrier, 
              execution_barrier, shard_manager->is_total_sharding(),
              shard_manager->is_first_local_shard(owner_shard));
          op->set_execution_precondition(precond);
          preconditions.insert(
              Runtime::protect_event(op->get_completion_event()));
          op->begin_dependence_analysis();
          for (std::map<Operation*,GenerationID>::const_iterator dit = 
                dependences.begin(); dit != dependences.end(); dit++)
            op->register_dependence(dit->first, dit->second);
          op->end_dependence_analysis();
        }
      }
    }

    //--------------------------------------------------------------------------
    CollectiveID ReplicateContext::get_next_collective_index(
                                      CollectiveIndexLocation loc, bool logical)
    //--------------------------------------------------------------------------
    {
      // No need for a lock, should only be coming from the creation
      // of operations directly from the application and therefore
      // should be deterministic
      // Count by 2s to avoid conflicts with the collectives from the 
      // logical depedence analysis stage of the pipeline
      if (logical)
      {
#ifdef DEBUG_LEGION_COLLECTIVES
        if (!logical_guard_reentrant)
        {
          CollectiveCheckReduction::RHS location = loc;
          Runtime::phase_barrier_arrive(logical_check_barrier, 1/*count*/,
                             RtEvent::NO_RT_EVENT, &location, sizeof(location));
          logical_check_barrier.wait();
          CollectiveCheckReduction::RHS actual_location;
          bool ready = Runtime::get_barrier_result(logical_check_barrier,
                                     &actual_location, sizeof(actual_location));
          assert(ready);
          assert(location == actual_location);
          // Guard against coming back in here when advancing the barrier
          logical_guard_reentrant = true;
          advance_logical_barrier(logical_check_barrier, total_shards);
          logical_guard_reentrant = false;
        }
#endif
        const CollectiveID result = next_logical_collective_index;
        next_logical_collective_index += 2;
        return result;
      }
      else
      {
#ifdef DEBUG_LEGION_COLLECTIVES
        if (!collective_guard_reentrant)
        {
          CollectiveCheckReduction::RHS location = loc;
          Runtime::phase_barrier_arrive(collective_check_barrier, 1/*count*/,
                             RtEvent::NO_RT_EVENT, &location, sizeof(location));
          collective_check_barrier.wait();
          CollectiveCheckReduction::RHS actual_location;
          bool ready = Runtime::get_barrier_result(collective_check_barrier,
                                     &actual_location, sizeof(actual_location));
          assert(ready);
          assert(location == actual_location);
          // Guard against coming back in here when advancing the barrier
          collective_guard_reentrant = true;
          advance_replicate_barrier(collective_check_barrier, total_shards);
          collective_guard_reentrant = false;
        }
#endif
        const CollectiveID result = next_available_collective_index;
        next_available_collective_index += 2; 
        return result;
      }
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_collective(ShardCollective *collective)
    //--------------------------------------------------------------------------
    {
      std::vector<std::pair<void*,size_t> > to_apply;
      {
        AutoLock repl_lock(replication_lock);
#ifdef DEBUG_LEGION
        assert(collectives.find(collective->collective_index) == 
               collectives.end());
        assert(shard_manager != NULL);
#endif
        // If the collectives are empty then we add a reference to the
        // shard manager to prevent it being collected before we're
        // done handling all the collectives
        if (collectives.empty())
          shard_manager->add_reference();
        collectives[collective->collective_index] = collective;
        std::map<CollectiveID,std::vector<std::pair<void*,size_t> > >::
          iterator finder = pending_collective_updates.find(
                                                collective->collective_index);
        if (finder != pending_collective_updates.end())
        {
          to_apply.swap(finder->second);
          pending_collective_updates.erase(finder);
        }
      }
      if (!to_apply.empty())
      {
        for (std::vector<std::pair<void*,size_t> >::const_iterator it = 
              to_apply.begin(); it != to_apply.end(); it++)
        {
          Deserializer derez(it->first, it->second);
          collective->handle_collective_message(derez);
          free(it->first);
        }
      }
    }

    //--------------------------------------------------------------------------
    ShardCollective* ReplicateContext::find_or_buffer_collective(
                                                            Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      CollectiveID collective_index;
      derez.deserialize(collective_index);
      AutoLock repl_lock(replication_lock);
      // See if we already have the collective in which case we can just
      // return it, otherwise we need to buffer the deserializer
      std::map<CollectiveID,ShardCollective*>::const_iterator finder = 
        collectives.find(collective_index);
      if (finder != collectives.end())
        return finder->second;
      // If we couldn't find it then we have to buffer it for the future
      const size_t remaining_bytes = derez.get_remaining_bytes();
      void *buffer = malloc(remaining_bytes);
      memcpy(buffer, derez.get_current_pointer(), remaining_bytes);
      derez.advance_pointer(remaining_bytes);
      pending_collective_updates[collective_index].push_back(
          std::pair<void*,size_t>(buffer, remaining_bytes));
      return NULL;
    } 

    //--------------------------------------------------------------------------
    void ReplicateContext::unregister_collective(ShardCollective *collective)
    //--------------------------------------------------------------------------
    {
      bool remove_reference = false;
      {
        AutoLock repl_lock(replication_lock); 
        std::map<CollectiveID,ShardCollective*>::iterator finder =
          collectives.find(collective->collective_index);
        // Sometimes collectives are not used
        if (finder != collectives.end())
        {
          collectives.erase(finder);
          // Once we've done all our collectives then we can remove the
          // reference that we added on the shard manager
          remove_reference = collectives.empty();
        }
      }
      if (remove_reference && shard_manager->remove_reference())
        delete shard_manager;
    }

    //--------------------------------------------------------------------------
    unsigned ReplicateContext::peek_next_future_map_barrier_index(void) const
    //--------------------------------------------------------------------------
    {
      return next_future_map_bar_index;
    }

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_future_map_barrier(void)
    //--------------------------------------------------------------------------
    {
      RtBarrier &next = future_map_barriers[next_future_map_bar_index++];
      if (next_future_map_bar_index == future_map_barriers.size())
        next_future_map_bar_index = 0;
      RtBarrier result = next;
      advance_replicate_barrier(next, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::register_future_map(ReplFutureMapImpl *map)
    //--------------------------------------------------------------------------
    {
      map->add_base_resource_ref(REPLICATION_REF);
      std::vector<std::pair<void*,size_t> > to_apply;
      {
        AutoLock repl_lock(replication_lock);
#ifdef DEBUG_LEGION
        assert(future_maps.find(map->future_map_barrier) == future_maps.end());
#endif
        future_maps[map->future_map_barrier] = map; 
        // Check to see if we have any pending requests to perform
        std::map<RtEvent,std::vector<std::pair<void*,size_t> > >::iterator
          finder = pending_future_map_requests.find(map->future_map_barrier);
        if (finder != pending_future_map_requests.end())
        {
          to_apply.swap(finder->second);
          pending_future_map_requests.erase(finder);
        }
      }
      if (!to_apply.empty())
      {
        for (std::vector<std::pair<void*,size_t> >::const_iterator it = 
              to_apply.begin(); it != to_apply.end(); it++)
        {
          Deserializer derez(it->first, it->second);
          map->handle_future_map_request(derez);
          free(it->first);
        }
      }
    }

    //--------------------------------------------------------------------------
    ReplFutureMapImpl* ReplicateContext::find_or_buffer_future_map_request(
                                                            Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      RtEvent future_map_event;
      derez.deserialize(future_map_event);
      AutoLock repl_lock(replication_lock);
      // See if we already have the future map in which case we can just
      // return it, otherwise we need to buffer the deserializer
      std::map<RtEvent,ReplFutureMapImpl*>::const_iterator finder = 
        future_maps.find(future_map_event);
      if (finder != future_maps.end())
        return finder->second;
      // If we couldn't find it then we have to buffer it for the future
      const size_t remaining_bytes = derez.get_remaining_bytes();
      void *buffer = malloc(remaining_bytes);
      memcpy(buffer, derez.get_current_pointer(), remaining_bytes);
      derez.advance_pointer(remaining_bytes);
      pending_future_map_requests[future_map_event].push_back(
          std::pair<void*,size_t>(buffer, remaining_bytes));
      return NULL;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::unregister_future_map(ReplFutureMapImpl *map)
    //--------------------------------------------------------------------------
    {
      {
        AutoLock repl_lock(replication_lock);
        std::map<RtEvent,ReplFutureMapImpl*>::iterator finder = 
          future_maps.find(map->future_map_barrier);
#ifdef DEBUG_LEGION
        assert(finder != future_maps.end());
#endif
        future_maps.erase(finder);
      }
      if (map->remove_base_resource_ref(REPLICATION_REF))
        delete map;
    } 

    //--------------------------------------------------------------------------
    size_t ReplicateContext::register_trace_template(
                                     ShardedPhysicalTemplate *physical_template)
    //--------------------------------------------------------------------------
    {
      size_t index;
      std::vector<PendingTemplateUpdate> to_apply;
      {
        AutoLock r_lock(replication_lock);
        index = next_physical_template_index++;
#ifdef DEBUG_LEGION
        assert(physical_templates.find(index) == physical_templates.end());
#endif
        physical_templates[index] = physical_template;
        // Check to see if we have any pending updates to perform
        std::map<size_t,std::vector<PendingTemplateUpdate> >::iterator
          finder = pending_template_updates.find(index);
        if (finder != pending_template_updates.end())
        {
          to_apply.swap(finder->second);
          pending_template_updates.erase(finder);
        }
      }
      if (!to_apply.empty())
      {
        for (std::vector<PendingTemplateUpdate>::const_iterator it = 
              to_apply.begin(); it != to_apply.end(); it++)
        {
          Deserializer derez(it->ptr, it->size);
          physical_template->handle_trace_update(derez, it->source);
          free(it->ptr);
        }
      }
      return index;
    }

    //--------------------------------------------------------------------------
    ShardedPhysicalTemplate* ReplicateContext::find_or_buffer_trace_update(
                                     Deserializer &derez, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      size_t trace_index;
      derez.deserialize(trace_index);
      AutoLock r_lock(replication_lock); 
      std::map<size_t,ShardedPhysicalTemplate*>::const_iterator finder = 
        physical_templates.find(trace_index);
      if (finder != physical_templates.end())
        return finder->second;
#ifdef DEBUG_LEGION
      assert(next_physical_template_index <= trace_index);
#endif
      // If we couldn't find it then we have to buffer it for the future
      const size_t remaining_bytes = derez.get_remaining_bytes();
      void *buffer = malloc(remaining_bytes);
      memcpy(buffer, derez.get_current_pointer(), remaining_bytes);
      derez.advance_pointer(remaining_bytes);
      pending_template_updates[trace_index].push_back(
          PendingTemplateUpdate(buffer, remaining_bytes, source));
      return NULL;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::unregister_trace_template(size_t index)
    //--------------------------------------------------------------------------
    {
      AutoLock r_lock(replication_lock);
#ifdef DEBUG_LEGION
      std::map<size_t,ShardedPhysicalTemplate*>::iterator finder = 
        physical_templates.find(index);
      assert(finder != physical_templates.end());
      physical_templates.erase(finder);
#else
      physical_templates.erase(index);
#endif
    }

    //--------------------------------------------------------------------------
    ShardID ReplicateContext::get_next_equivalence_set_origin(void)
    //--------------------------------------------------------------------------
    {
      const ShardID result = equivalence_set_allocator_shard++;
      if (equivalence_set_allocator_shard == total_shards)
        equivalence_set_allocator_shard = 0;
      return result;
    }

    //--------------------------------------------------------------------------
    bool ReplicateContext::replicate_partition_equivalence_sets(
                                                      PartitionNode *node) const
    //--------------------------------------------------------------------------
    {
      // This is the heuristic that decides whether or not we should replicate
      // or shard the equivalence sets for a partition node
      // For now we'll shard as long as there are at least as many children
      // in the partition as there are shards, otherwise we'll replicate
      return (node->get_num_children() < total_shards);
    }

    //--------------------------------------------------------------------------
    bool ReplicateContext::finalize_disjoint_complete_sets(RegionNode *region, 
            VersionManager *target, FieldMask request_mask, const UniqueID opid,
            const AddressSpaceID source, RtUserEvent ready_event)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(region->parent != NULL);
      assert(!replicate_partition_equivalence_sets(region->parent));
#endif
      // Determine whether we should own the equivalence set data for
      // this region node or not
      IndexPartNode *index_part = region->parent->row_source;
      // See if we can find its shard owner the easy way or the hard way
      ShardID target_shard;
      const LegionColor color = region->get_color();
      if (index_part->total_children != index_part->max_linearized_color)
      {
        // Have to do this the hard way
        const size_t index_offset = 
          index_part->color_space->compute_color_offset(color);
        target_shard = index_offset % total_shards;
      }
      else // This is the easy way, we can just linearize the color 
        target_shard = color % total_shards;
      if (target_shard != owner_shard->shard_id)
      {
        // We're not the owner so forward this to the owner shard
        Serializer rez;
        rez.serialize(shard_manager->repl_id);
        rez.serialize(target_shard);
        rez.serialize(region->handle);
        rez.serialize(target);
        rez.serialize(runtime->address_space);
        rez.serialize(request_mask);
        rez.serialize(opid);
        rez.serialize(source);
        rez.serialize(ready_event);
        shard_manager->send_disjoint_complete_request(target_shard, rez);
        return false;
      }
      else // we're the owner so just handle this here
        return InnerContext::finalize_disjoint_complete_sets(region, target,
                                    request_mask, opid, source, ready_event);
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::deduplicate_invalidate_trackers(
                                 const FieldMaskSet<EquivalenceSet> &to_untrack,
                                 std::set<RtEvent> &applied_events)
    //--------------------------------------------------------------------------
    {
      // check to see if we're the first shard on this node
      const bool first_local_shard = 
        shard_manager->is_first_local_shard(owner_shard);
      const CollectiveMapping &collective_mapping = 
        shard_manager->get_collective_mapping();
      for (FieldMaskSet<EquivalenceSet>::const_iterator it =
            to_untrack.begin(); it != to_untrack.end(); it++)
      {
        if (it->first->collective_mapping != NULL)
        {
          // This was an equivalence set that was made by all shards
          // so we can invalidate it from just the first shards on
          // each node with the collective manager map
          if (first_local_shard)
            it->first->invalidate_trackers(it->second, applied_events,
                runtime->address_space, &collective_mapping);
        }
        else
        {
          // This was an equivalence set that was made on just one shard
          RegionNode *region = it->first->region_node;
          const LegionColor color = region->get_color();
          ShardID target_shard;
          if (region->parent != NULL)
          {
            IndexPartNode *index_part = region->parent->row_source;
            if (index_part->total_children != index_part->max_linearized_color)
            {
              // Have to do this the hard way
              const size_t index_offset = 
                index_part->color_space->compute_color_offset(color);
              target_shard = index_offset % total_shards;
            }
            else // This is the easy way, we can just linearize the color 
              target_shard = color % total_shards;
          }
          else
            target_shard = color % total_shards;
          if (target_shard == owner_shard->shard_id)
            it->first->invalidate_trackers(it->second, applied_events,
                  runtime->address_space, NULL/*collective manager*/);
        }
      }
    }

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_mapping_fence_barrier(void)
    //--------------------------------------------------------------------------
    {
      RtBarrier result = mapping_fence_barrier;
      advance_logical_barrier(mapping_fence_barrier, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    ApBarrier ReplicateContext::get_next_execution_fence_barrier(void)
    //--------------------------------------------------------------------------
    {
      ApBarrier result = execution_fence_barrier;
      advance_logical_barrier(execution_fence_barrier, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_resource_return_barrier(void)
    //--------------------------------------------------------------------------
    {
      RtBarrier result = resource_return_barrier;
      advance_replicate_barrier(resource_return_barrier, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_refinement_barrier(void)
    //--------------------------------------------------------------------------
    {
      const unsigned refinement_index = next_refinement_ready_bar_index++;
      if (next_refinement_ready_bar_index == refinement_ready_barriers.size())
        next_refinement_ready_bar_index = 0;
      RtBarrier &refinement_bar = refinement_ready_barriers[refinement_index];
#ifdef DEBUG_LEGION_COLLECTIVES
      CloseCheckReduction::RHS barrier(user, refinement_bar,
                                       node, false/*read only*/);
      Runtime::phase_barrier_arrive(refinement_check_barrier, 1/*count*/,
                              RtEvent::NO_RT_EVENT, &barrier, sizeof(barrier));
      refinement_check_barrier.wait();
      CloseCheckReduction::RHS actual_barrier;
      bool ready = Runtime::get_barrier_result(refinement_check_barrier,
                                      &actual_barrier, sizeof(actual_barrier));
      assert(ready);
      assert(actual_barrier == barrier);
      advance_logical_barrier(refinement_check_barrier, total_shards);
#endif
      const RtBarrier result = refinement_bar;
      advance_logical_barrier(refinement_bar, total_shards);
      return result;
    } 

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_trace_recording_barrier(void)
    //--------------------------------------------------------------------------
    {
      const RtBarrier result = trace_recording_barrier;
      advance_logical_barrier(trace_recording_barrier, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    RtBarrier ReplicateContext::get_next_summary_fence_barrier(void)
    //--------------------------------------------------------------------------
    {
      const RtBarrier result = summary_fence_barrier;
      advance_logical_barrier(summary_fence_barrier, total_shards);
      return result;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_new_replicate_barrier(RtBarrier &bar, 
                                                        size_t arrivals)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!bar.exists());
      assert(next_replicate_bar_index < total_shards);
#endif
      ValueBroadcast<RtBarrier> 
        collective(this, next_replicate_bar_index, COLLECTIVE_LOC_83);
      if (owner_shard->shard_id == next_replicate_bar_index++)
      {
        bar = RtBarrier(Realm::Barrier::create_barrier(arrivals));
        collective.broadcast(bar);
      }
      else
        bar = collective.get_value();
      // Check to see if we need to reset the next_replicate_bar_index
      if (next_replicate_bar_index == total_shards)
        next_replicate_bar_index = 0;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_new_replicate_barrier(ApBarrier &bar,
                                                        size_t arrivals)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!bar.exists());
      assert(next_replicate_bar_index < total_shards);
#endif
      ValueBroadcast<ApBarrier> 
        collective(this, next_replicate_bar_index, COLLECTIVE_LOC_84);
      if (owner_shard->shard_id == next_replicate_bar_index++)
      {
        bar = ApBarrier(Realm::Barrier::create_barrier(arrivals));
        collective.broadcast(bar);
      }
      else
        bar = collective.get_value();
      // Check to see if we need to reset the next_replicate_bar_index
      if (next_replicate_bar_index == total_shards)
        next_replicate_bar_index = 0;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_new_logical_barrier(RtBarrier &bar, 
                                                      size_t arrivals)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!bar.exists());
      assert(next_logical_bar_index < total_shards);
#endif
      const CollectiveID cid =
        get_next_collective_index(COLLECTIVE_LOC_18, true/*logical*/);
      ValueBroadcast<RtBarrier> collective(cid, this, next_logical_bar_index);
      if (owner_shard->shard_id == next_logical_bar_index++)
      {
        bar = RtBarrier(Realm::Barrier::create_barrier(arrivals));
        collective.broadcast(bar);
      }
      else
        bar = collective.get_value();
      // Check to see if we need to reset the next_replicate_bar_index
      if (next_logical_bar_index == total_shards)
        next_logical_bar_index = 0;
    }

    //--------------------------------------------------------------------------
    void ReplicateContext::create_new_logical_barrier(ApBarrier &bar, 
                                                      size_t arrivals)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!bar.exists());
      assert(next_logical_bar_index < total_shards);
#endif
      const CollectiveID cid =
        get_next_collective_index(COLLECTIVE_LOC_24, true/*logical*/);
      ValueBroadcast<ApBarrier> collective(cid, this, next_logical_bar_index);
      if (owner_shard->shard_id == next_logical_bar_index++)
      {
        bar = ApBarrier(Realm::Barrier::create_barrier(arrivals));
        collective.broadcast(bar);
      }
      else
        bar = collective.get_value();
      // Check to see if we need to reset the next_replicate_bar_index
      if (next_logical_bar_index == total_shards)
        next_logical_bar_index = 0;
    }

    /////////////////////////////////////////////////////////////
    // Remote Task 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    RemoteTask::RemoteTask(RemoteContext *own)
      : owner(own), context_index(0)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    RemoteTask::RemoteTask(const RemoteTask &rhs)
      : owner(NULL)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    RemoteTask::~RemoteTask(void)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    RemoteTask& RemoteTask::operator=(const RemoteTask &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    UniqueID RemoteTask::get_unique_id(void) const
    //--------------------------------------------------------------------------
    {
      return owner->get_context_uid();
    }

    //--------------------------------------------------------------------------
    size_t RemoteTask::get_context_index(void) const
    //--------------------------------------------------------------------------
    {
      return context_index;
    }

    //--------------------------------------------------------------------------
    void RemoteTask::set_context_index(size_t index)
    //--------------------------------------------------------------------------
    {
      context_index = index;
    }

    //--------------------------------------------------------------------------
    bool RemoteTask::has_parent_task(void) const
    //--------------------------------------------------------------------------
    {
      return (get_depth() > 0);
    }

    //--------------------------------------------------------------------------
    const Task* RemoteTask::get_parent_task(void) const
    //--------------------------------------------------------------------------
    {
      if ((parent_task == NULL) && has_parent_task())
        parent_task = owner->get_parent_task();
      return parent_task;
    }
    
    //--------------------------------------------------------------------------
    int RemoteTask::get_depth(void) const
    //--------------------------------------------------------------------------
    {
      return owner->get_depth();
    }

    //--------------------------------------------------------------------------
    const char* RemoteTask::get_task_name(void) const
    //--------------------------------------------------------------------------
    {
      TaskImpl *task_impl = owner->runtime->find_task_impl(task_id);
      return task_impl->get_name();
    }

    //--------------------------------------------------------------------------
    bool RemoteTask::has_trace(void) const
    //--------------------------------------------------------------------------
    {
      return false;
    }

    /////////////////////////////////////////////////////////////
    // Remote Context 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    RemoteContext::RemoteContext(Runtime *rt, UniqueID context_uid)
      : InnerContext(rt, NULL, -1, false/*full inner*/, remote_task.regions,
                     remote_task.output_regions, local_parent_req_indexes,
                     local_virtual_mapped, context_uid, ApEvent::NO_AP_EVENT,
                     true/*remote*/),
        parent_ctx(NULL), shard_manager(NULL),
        top_level_context(false), remote_task(RemoteTask(this)), repl_id(0)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    RemoteContext::RemoteContext(const RemoteContext &rhs)
      : InnerContext(NULL, NULL, 0, false, rhs.regions, rhs.output_reqs,
                     local_parent_req_indexes, local_virtual_mapped, 0,
                     ApEvent::NO_AP_EVENT, true),
        remote_task(RemoteTask(this))
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    RemoteContext::~RemoteContext(void)
    //--------------------------------------------------------------------------
    {
      // At this point we can free our region tree context
      runtime->free_region_tree_context(tree_context);
      if (!local_field_infos.empty())
      {
        // If we have any local fields then tell field space that
        // we can remove them and then clear them 
        for (std::map<FieldSpace,std::vector<LocalFieldInfo> >::const_iterator
              it = local_field_infos.begin(); 
              it != local_field_infos.end(); it++)
        {
          const std::vector<LocalFieldInfo> &infos = it->second;
          std::vector<FieldID> to_remove;
          for (unsigned idx = 0; idx < infos.size(); idx++)
          {
            if (infos[idx].ancestor)
              continue;
            to_remove.push_back(infos[idx].fid);
          }
          if (!to_remove.empty())
            runtime->forest->remove_local_fields(it->first, to_remove);
        }
        local_field_infos.clear();
      } 
    }

    //--------------------------------------------------------------------------
    RemoteContext& RemoteContext::operator=(const RemoteContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    Task* RemoteContext::get_task(void)
    //--------------------------------------------------------------------------
    {
      return &remote_task;
    }

    //--------------------------------------------------------------------------
    InnerContext* RemoteContext::find_top_context(InnerContext *previous)
    //--------------------------------------------------------------------------
    {
      if (!top_level_context)
        return find_parent_context()->find_top_context(this);
 #ifdef DEBUG_LEGION
      assert(previous != NULL);
#endif
      return previous;     
    }
    
    //--------------------------------------------------------------------------
    InnerContext* RemoteContext::find_parent_context(void)
    //--------------------------------------------------------------------------
    {
      if (top_level_context)
        return NULL;
      // See if we already have it
      if (parent_ctx != NULL)
        return parent_ctx;
#ifdef DEBUG_LEGION
      assert(parent_context_uid != 0);
#endif
      // THIS IS ONLY SAFE BECAUSE THIS FUNCTION IS NEVER CALLED BY
      // A MESSAGE IN THE CONTEXT_VIRTUAL_CHANNEL
      parent_ctx = runtime->find_context(parent_context_uid);
#ifdef DEBUG_LEGION
      assert(parent_ctx != NULL);
#endif
      remote_task.parent_task = parent_ctx->get_task();
      return parent_ctx;
    }

    //--------------------------------------------------------------------------
    RtEvent RemoteContext::compute_equivalence_sets(EqSetTracker *target,
                      AddressSpaceID target_space, RegionNode *region,
                      const FieldMask &mask, const UniqueID opid,
                      const AddressSpaceID original_source)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!top_level_context);
      assert(target_space == runtime->address_space); // should always be local
#endif
      // Send it to the owner space if we are the top-level context
      // otherwise we send it to the owner of the context
      const AddressSpaceID dest = runtime->get_runtime_owner(context_uid);
      RtUserEvent ready_event = Runtime::create_rt_user_event();
      // Send off a request to the owner node to handle it
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(context_uid);
        rez.serialize(target);
        rez.serialize(region->handle);
        rez.serialize(mask);
        rez.serialize(opid);
        rez.serialize(original_source);
        rez.serialize(ready_event);
      }
      // Send it to the owner space 
      runtime->send_compute_equivalence_sets_request(dest, rez);
      return ready_event;
    }

    //--------------------------------------------------------------------------
    InnerContext* RemoteContext::find_parent_physical_context(unsigned index)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(regions.size() == virtual_mapped.size());
      assert(regions.size() == parent_req_indexes.size());
#endif     
      if (index < virtual_mapped.size())
      {
        // See if it is virtual mapped
        if (virtual_mapped[index])
          return find_parent_context()->find_parent_physical_context(
                                            parent_req_indexes[index]);
        else // We mapped a physical instance so we're it
          return this;
      }
      else // We created it
      {
        // But we're the remote note, so we don't have updated created
        // requirements or returnable privileges so we need to see if
        // we already know the answer and if not, ask the owner context
        RtEvent wait_on;
        RtUserEvent request;
        {
          AutoLock rem_lock(remote_lock);
          std::map<unsigned,InnerContext*>::const_iterator finder = 
            physical_contexts.find(index);
          if (finder != physical_contexts.end())
            return finder->second;
          std::map<unsigned,RtEvent>::const_iterator pending_finder = 
            pending_physical_contexts.find(index);
          if (pending_finder == pending_physical_contexts.end())
          {
            // Make a new request
            request = Runtime::create_rt_user_event();
            pending_physical_contexts[index] = request;
            wait_on = request;
          }
          else // Already sent it so just get the wait event
            wait_on = pending_finder->second;
        }
        if (request.exists())
        {
          // Send the request
          Serializer rez;
          {
            RezCheck z(rez);
            rez.serialize(context_uid);
            rez.serialize(index);
            rez.serialize(this);
            rez.serialize(request);
          }
          const AddressSpaceID target = runtime->get_runtime_owner(context_uid);
          runtime->send_remote_context_physical_request(target, rez);
        }
        // Wait for the result to come back to us
        wait_on.wait();
        // When we wake up it should be there
        AutoLock rem_lock(remote_lock, 1, false/*exclusive*/);
#ifdef DEBUG_LEGION
        assert(physical_contexts.find(index) != physical_contexts.end());
#endif
        return physical_contexts[index]; 
      }
    }

    //--------------------------------------------------------------------------
    InstanceView* RemoteContext::create_instance_top_view(
                                PhysicalManager* manager, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      // Check to see if we are part of a replicate context, if we do
      // then we need to send this request back to our owner node
      if (repl_id > 0)
      {
        InstanceView *volatile result = NULL;
        RtUserEvent wait_on = Runtime::create_rt_user_event();
        Serializer rez;
        {
          RezCheck z(rez);
          rez.serialize<UniqueID>(context_uid);
          rez.serialize(manager->did);
          rez.serialize<InstanceView**>(const_cast<InstanceView**>(&result));
          rez.serialize(wait_on); 
        }
        const AddressSpaceID target = runtime->get_runtime_owner(context_uid);
        runtime->send_create_top_view_request(target, rez);
        wait_on.wait();
#ifdef DEBUG_LEGION
        assert(result != NULL);
#endif
        return result;
      }
      else
        return InnerContext::create_instance_top_view(manager, source);
    }

    //--------------------------------------------------------------------------
    void RemoteContext::invalidate_region_tree_contexts(
                       const bool is_top_level_task, std::set<RtEvent> &applied)
    //--------------------------------------------------------------------------
    {
      // Should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    void RemoteContext::receive_created_region_contexts(RegionTreeContext ctx,
                           const std::vector<RegionNode*> &created_state,
                           std::set<RtEvent> &applied_events, size_t num_shards,
                           InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      std::vector<DistributedCollectable*> remove_remote_references;
      const RtUserEvent done_event = Runtime::create_rt_user_event();
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(context_uid);
        rez.serialize<size_t>(num_shards);
        rez.serialize<size_t>(created_state.size());
        for (std::vector<RegionNode*>::const_iterator it = 
              created_state.begin(); it != created_state.end(); it++)
        {
          rez.serialize((*it)->handle);
          (*it)->pack_logical_state(ctx.get_id(), rez, true/*invalidate*/, 
                                    remove_remote_references);
          (*it)->pack_version_state(ctx.get_id(), rez, true/*invalidate*/, 
                applied_events, source_context, remove_remote_references);
        }
        rez.serialize(done_event);
      }
      const AddressSpaceID target = runtime->get_runtime_owner(context_uid);
      runtime->send_created_region_contexts(target, rez);
      applied_events.insert(done_event);
      if (!remove_remote_references.empty())
      { 
        if (!done_event.has_triggered())
        {
          std::vector<DistributedCollectable*> *to_remove = 
            new std::vector<DistributedCollectable*>();
          to_remove->swap(remove_remote_references);
          DeferRemoveRemoteReferenceArgs args(context_uid, to_remove);
          runtime->issue_runtime_meta_task(args, 
              LG_LATENCY_DEFERRED_PRIORITY, done_event); 
        }
        else
          InnerContext::remove_remote_references(remove_remote_references);
      }
    }

    //--------------------------------------------------------------------------
    void RemoteContext::free_region_tree_context(void)
    //--------------------------------------------------------------------------
    {
      // Should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::handle_created_region_contexts(
                   Runtime *runtime, Deserializer &derez, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      UniqueID ctx_uid;
      derez.deserialize(ctx_uid);
      const RegionTreeContext ctx = runtime->allocate_region_tree_context();
      size_t num_shards;
      derez.deserialize(num_shards);
      size_t num_regions;
      derez.deserialize(num_regions);
      std::vector<RegionNode*> created_state(num_regions);
      std::set<RtEvent> applied_events;
      for (unsigned idx = 0; idx < num_regions; idx++)
      {
        LogicalRegion handle;
        derez.deserialize(handle);
        RegionNode *node = runtime->forest->get_node(handle);
        node->unpack_logical_state(ctx.get_id(), derez, source);
        node->unpack_version_state(ctx.get_id(), derez, source);
        created_state[idx] = node;
      }
      RtUserEvent done_event;
      derez.deserialize(done_event);

      InnerContext *context = runtime->find_context(ctx_uid);
      context->receive_created_region_contexts(ctx, created_state, 
              applied_events, num_shards, NULL/*source context*/);
      if (!applied_events.empty())
        Runtime::trigger_event(done_event, 
            Runtime::merge_events(applied_events));
      else
        Runtime::trigger_event(done_event);
      runtime->free_region_tree_context(ctx); 
    }

    //--------------------------------------------------------------------------
    ShardingFunction* RemoteContext::find_sharding_function(ShardingID sid)
    //--------------------------------------------------------------------------
    {
      if (shard_manager != NULL)
        return shard_manager->find_sharding_function(sid);
      // Check to see if it is in the cache
      {
        AutoLock rem_lock(remote_lock,1,false/*exclusive*/);
        std::map<ShardingID,ShardingFunction*>::const_iterator finder = 
          sharding_functions.find(sid);
        if (finder != sharding_functions.end())
          return finder->second;
      }
      // Get the functor from the runtime
      ShardingFunctor *functor = runtime->find_sharding_functor(sid);
      // Retake the lock
      AutoLock rem_lock(remote_lock);
      // See if we lost the race
      std::map<ShardingID,ShardingFunction*>::const_iterator finder = 
        sharding_functions.find(sid);
      if (finder != sharding_functions.end())
        return finder->second;
      ShardingFunction *result = 
        new ShardingFunction(functor, runtime->forest, sid, total_shards);
      // Save the result for the future
      sharding_functions[sid] = result;
      return result;
    }

    //--------------------------------------------------------------------------
    void RemoteContext::unpack_remote_context(Deserializer &derez,
                                              std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      DETAILED_PROFILER(runtime, REMOTE_UNPACK_CONTEXT_CALL);
      derez.deserialize(depth);
      top_level_context = (depth < 0);
      // If we're the top-level context then we're already done
      if (top_level_context)
        return;
      WrapperReferenceMutator mutator(preconditions);
      remote_task.unpack_external_task(derez, runtime, &mutator);
      local_parent_req_indexes.resize(remote_task.regions.size()); 
      for (unsigned idx = 0; idx < local_parent_req_indexes.size(); idx++)
        derez.deserialize(local_parent_req_indexes[idx]);
      size_t num_virtual;
      derez.deserialize(num_virtual);
      local_virtual_mapped.resize(regions.size(), false);
      for (unsigned idx = 0; idx < num_virtual; idx++)
      {
        unsigned index;
        derez.deserialize(index);
        local_virtual_mapped[index] = true;
      }
      derez.deserialize(parent_context_uid);
      size_t num_coordinates;
      derez.deserialize(num_coordinates);
      context_coordinates.resize(num_coordinates);
      for (unsigned idx = 0; idx < num_coordinates; idx++)
      {
        std::pair<size_t,DomainPoint> &coordinate = context_coordinates[idx];
        derez.deserialize(coordinate.first);
        derez.deserialize(coordinate.second);
      }
      // Unpack any local fields that we have
      unpack_local_field_update(derez);
      bool replicate;
      derez.deserialize(replicate);
      if (replicate)
      {
        derez.deserialize(total_shards);
        derez.deserialize(repl_id);
        // See if we have a local shard manager
        shard_manager = runtime->find_shard_manager(repl_id, true/*can fail*/);
      }
      // See if we can find our parent task, if not don't worry about it
      // DO NOT CHANGE THIS UNLESS YOU THINK REALLY HARD ABOUT VIRTUAL 
      // CHANNELS AND HOW CONTEXT META-DATA IS MOVED!
      parent_ctx = runtime->find_context(parent_context_uid, true/*can fail*/);
      if (parent_ctx != NULL)
        remote_task.parent_task = parent_ctx->get_task();
    }

    //--------------------------------------------------------------------------
    const Task* RemoteContext::get_parent_task(void)
    //--------------------------------------------------------------------------
    {
      // Note that it safe to actually perform the find_context call here
      // because we are no longer in the virtual channel for unpacking
      // remote contexts therefore we can page in the context
      if (parent_ctx == NULL)
        parent_ctx = runtime->find_context(parent_context_uid);
      return parent_ctx->get_task();
    }

    //--------------------------------------------------------------------------
    void RemoteContext::unpack_local_field_update(Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      size_t num_field_spaces;
      derez.deserialize(num_field_spaces);
      if (num_field_spaces == 0)
        return;
      for (unsigned fidx = 0; fidx < num_field_spaces; fidx++)
      {
        FieldSpace handle;
        derez.deserialize(handle);
        size_t num_local;
        derez.deserialize(num_local); 
        std::vector<FieldID> fields(num_local);
        std::vector<size_t> field_sizes(num_local);
        std::vector<CustomSerdezID> serdez_ids(num_local);
        std::vector<unsigned> indexes(num_local);
        {
          // Take the lock for updating this data structure
          AutoLock local_lock(local_field_lock);
          std::vector<LocalFieldInfo> &infos = local_field_infos[handle];
          infos.resize(num_local);
          for (unsigned idx = 0; idx < num_local; idx++)
          {
            LocalFieldInfo &info = infos[idx];
            derez.deserialize(info);
            // Update data structures for notifying the field space
            fields[idx] = info.fid;
            field_sizes[idx] = info.size;
            serdez_ids[idx] = info.serdez;
            indexes[idx] = info.index;
          }
        }
        runtime->forest->update_local_fields(handle, fields, field_sizes,
                                             serdez_ids, indexes);
      }
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::handle_local_field_update(
                                                            Deserializer &derez)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      RemoteContext *context;
      derez.deserialize(context);
      context->unpack_local_field_update(derez);
      RtUserEvent done_event;
      derez.deserialize(done_event);
      Runtime::trigger_event(done_event);
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::handle_physical_request(Deserializer &derez,
                                        Runtime *runtime, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      UniqueID context_uid;
      derez.deserialize(context_uid);
      unsigned index;
      derez.deserialize(index);
      RemoteContext *target;
      derez.deserialize(target);
      RtUserEvent to_trigger;
      derez.deserialize(to_trigger);
      RtEvent ctx_ready;
      InnerContext *local = 
        runtime->find_context(context_uid, false/*can fail*/, &ctx_ready);

      // Always defer this in case it blocks, we can't block the virtual channel
      RemotePhysicalRequestArgs args(context_uid, target, local, 
                                     index, source, to_trigger);
      runtime->issue_runtime_meta_task(args, 
          LG_LATENCY_DEFERRED_PRIORITY, ctx_ready);
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::defer_physical_request(const void *args,
                                                          Runtime *runtime)
    //--------------------------------------------------------------------------
    {
      const RemotePhysicalRequestArgs *rargs = 
        (const RemotePhysicalRequestArgs*)args;
      InnerContext *result = 
        rargs->local->find_parent_physical_context(rargs->index);
      Serializer rez;
      {
        RezCheck z(rez);
        rez.serialize(rargs->target);
        rez.serialize(rargs->index);
        rez.serialize(result->context_uid);
        rez.serialize(rargs->to_trigger);
      }
      runtime->send_remote_context_physical_response(rargs->source, rez);
    }

    //--------------------------------------------------------------------------
    void RemoteContext::set_physical_context_result(unsigned index,
                                                    InnerContext *result)
    //--------------------------------------------------------------------------
    {
      AutoLock rem_lock(remote_lock);
#ifdef DEBUG_LEGION
      assert(physical_contexts.find(index) == physical_contexts.end());
#endif
      physical_contexts[index] = result;
      std::map<unsigned,RtEvent>::iterator finder = 
        pending_physical_contexts.find(index);
#ifdef DEBUG_LEGION
      assert(finder != pending_physical_contexts.end());
#endif
      pending_physical_contexts.erase(finder);
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::handle_physical_response(Deserializer &derez,
                                                            Runtime *runtime)
    //--------------------------------------------------------------------------
    {
      DerezCheck z(derez);
      RemoteContext *target;
      derez.deserialize(target);
      unsigned index;
      derez.deserialize(index);
      UniqueID result_uid;
      derez.deserialize(result_uid);
      RtUserEvent to_trigger;
      derez.deserialize(to_trigger);
      RtEvent ctx_ready;
      InnerContext *result = 
        runtime->find_context(result_uid, false/*weak*/, &ctx_ready);
      if (ctx_ready.exists())
      {
        // Launch a continuation in case we need to page in the context
        // We obviously can't block the virtual channel
        RemotePhysicalResponseArgs args(target, result, index);
        RtEvent done = 
          runtime->issue_runtime_meta_task(args, LG_LATENCY_DEFERRED_PRIORITY);
        Runtime::trigger_event(to_trigger, done);
      }
      else
      {
        target->set_physical_context_result(index, result);
        Runtime::trigger_event(to_trigger);
      }
    }

    //--------------------------------------------------------------------------
    /*static*/ void RemoteContext::defer_physical_response(const void *args)
    //--------------------------------------------------------------------------
    {
      const RemotePhysicalResponseArgs *rargs = 
        (const RemotePhysicalResponseArgs*)args;
      rargs->target->set_physical_context_result(rargs->index, rargs->result);
    }

    /////////////////////////////////////////////////////////////
    // Leaf Context 
    /////////////////////////////////////////////////////////////

    //--------------------------------------------------------------------------
    LeafContext::LeafContext(Runtime *rt, SingleTask *owner, bool inline_task)
      : TaskContext(rt, owner, owner->get_depth(), owner->regions,
                    owner->output_regions, inline_task),
        inlined_tasks(0)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    LeafContext::LeafContext(const LeafContext &rhs)
      : TaskContext(NULL, NULL, 0, rhs.regions, rhs.output_reqs, false)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    LeafContext::~LeafContext(void)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    LeafContext& LeafContext::operator=(const LeafContext &rhs)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
      return *this;
    }

    //--------------------------------------------------------------------------
    void LeafContext::receive_resources(size_t return_index,
              std::map<LogicalRegion,unsigned> &created_regs,
              std::vector<LogicalRegion> &deleted_regs,
              std::set<std::pair<FieldSpace,FieldID> > &created_fids,
              std::vector<std::pair<FieldSpace,FieldID> > &deleted_fids,
              std::map<FieldSpace,unsigned> &created_fs,
              std::map<FieldSpace,std::set<LogicalRegion> > &latent_fs,
              std::vector<FieldSpace> &deleted_fs,
              std::map<IndexSpace,unsigned> &created_is,
              std::vector<std::pair<IndexSpace,bool> > &deleted_is,
              std::map<IndexPartition,unsigned> &created_partitions,
              std::vector<std::pair<IndexPartition,bool> > &deleted_partitions,
              std::set<RtEvent> &preconditions)
    //--------------------------------------------------------------------------
    {
      // should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    RegionTreeContext LeafContext::get_context(void) const
    //--------------------------------------------------------------------------
    {
      assert(false);
      return RegionTreeContext();
    }

    //--------------------------------------------------------------------------
    ContextID LeafContext::get_context_id(void) const
    //--------------------------------------------------------------------------
    {
      assert(false);
      return 0;
    }

    //--------------------------------------------------------------------------
    void LeafContext::pack_remote_context(Serializer &rez,
                                          AddressSpaceID target, bool replicate)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::compute_task_tree_coordinates(
                       std::vector<std::pair<size_t,DomainPoint> > &coordinates)
    //--------------------------------------------------------------------------
    {
      TaskContext *owner_ctx = owner_task->get_context();
#ifdef DEBUG_LEGION
      InnerContext *parent_ctx = dynamic_cast<InnerContext*>(owner_ctx);
      assert(parent_ctx != NULL);
#else
      InnerContext *parent_ctx = static_cast<InnerContext*>(owner_ctx);
#endif
      parent_ctx->compute_task_tree_coordinates(coordinates);
      coordinates.push_back(std::make_pair(
            owner_task->get_context_index(), owner_task->index_point));
    }

    //--------------------------------------------------------------------------
    bool LeafContext::attempt_children_complete(void)
    //--------------------------------------------------------------------------
    {
      AutoLock leaf(leaf_lock);
      if (!children_complete_invoked)
      {
        children_complete_invoked = true;
        return true;
      }
      return false;
    }

    //--------------------------------------------------------------------------
    bool LeafContext::attempt_children_commit(void)
    //--------------------------------------------------------------------------
    {
      AutoLock leaf(leaf_lock);
      if (!children_commit_invoked)
      {
        children_commit_invoked = true;
        return true;
      }
      return false;
    }

    //--------------------------------------------------------------------------
    void LeafContext::inline_child_task(TaskOp *child)
    //--------------------------------------------------------------------------
    {
      if (runtime->check_privileges)
        child->perform_privilege_checks();
      if (runtime->legion_spy_enabled)
        LegionSpy::log_inline_task(child->get_unique_id());
      // Find the mapped physical regions associated with each of the
      // child task's region requirements. If they aren't mapped then
      // we need a mapping fence to ensure that all the mappings are
      // done before we attempt to run this task. If they are all mapped
      // though then we can run this right away.
      std::vector<PhysicalRegion> child_regions(child->regions.size());
      for (unsigned childidx = 0; childidx < child_regions.size(); childidx++)
      {
        const RegionRequirement &child_req = child->regions[childidx];
#ifdef DEBUG_LEGION
        bool found = false;
#endif
        for (unsigned our_idx = 0; our_idx < physical_regions.size(); our_idx++)
        {
          if (!physical_regions[our_idx].is_mapped())
            continue;
          const RegionRequirement &our_req = regions[our_idx];
          const RegionTreeID our_tid = our_req.region.get_tree_id();
          const IndexSpace our_space = our_req.region.get_index_space();
          const RegionUsage our_usage(our_req);
          if (!check_region_dependence(our_tid, our_space, our_req,
                  our_usage, child_req, false/*ignore privileges*/))
            continue;
          child_regions[childidx] = physical_regions[our_idx];
#ifdef DEBUG_LEGION
          found = true;
#endif
          break;
        }
#ifdef DEBUG_LEGION
        assert(found);
#endif
      }
      // Now select the variant for task based on the regions 
      std::deque<InstanceSet> physical_instances(child_regions.size());
      VariantImpl *variant = 
        select_inline_variant(child, child_regions, physical_instances); 
      child->perform_inlining(variant, physical_instances);
      // No need to wait here, we know everything are leaves all the way
      // down from here so there is no way for there to be effects
    }

    //--------------------------------------------------------------------------
    VariantImpl* LeafContext::select_inline_variant(TaskOp *child,
                              const std::vector<PhysicalRegion> &parent_regions,
                              std::deque<InstanceSet> &physical_instances)
    //--------------------------------------------------------------------------
    {
      VariantImpl *variant_impl = TaskContext::select_inline_variant(child,
                                        parent_regions, physical_instances);
      if (!variant_impl->is_leaf())
      {
        MapperManager *child_mapper = 
          runtime->find_mapper(executing_processor, child->map_id);
        REPORT_LEGION_ERROR(ERROR_INVALID_MAPPER_OUTPUT,
                      "Invalid mapper output from invoction of "
                      "'select_task_variant' on mapper %s. Mapper selected "
                      "an invalid variant ID %d for inlining of task %s "
                      "(UID %lld). Parent task %s (UID %lld) is a leaf task "
                      "but mapper selected non-leaf variant %d for task %s.",
                      child_mapper->get_mapper_name(),
                      variant_impl->vid, child->get_task_name(), 
                      child->get_unique_id(), owner_task->get_task_name(),
                      owner_task->get_unique_id(), variant_impl->vid,
                      child->get_task_name())
      }
      return variant_impl;
    }

    //--------------------------------------------------------------------------
    void LeafContext::handle_registration_callback_effects(RtEvent effects)
    //--------------------------------------------------------------------------
    {
      if (effects.has_triggered())
        return;
      AutoLock l_lock(leaf_lock);
      execution_events.insert(effects);
    }

    //--------------------------------------------------------------------------
    bool LeafContext::is_leaf_context(void) const
    //--------------------------------------------------------------------------
    {
      return true;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space(const Future &f, TypeTag tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
        "Illegal index space from future creation performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexSpace::NO_SPACE;
    } 

    //--------------------------------------------------------------------------
    void LeafContext::destroy_index_space(IndexSpace handle, 
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      // Check to see if this is a top-level index space, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_index_space(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy index space %x in task %s (UID %lld) "
            "which is not a top-level index space. Legion only permits "
            "top-level index spaces to be destroyed.", handle.get_id(),
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      bool has_created = true;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexSpace,unsigned>::iterator finder = 
          created_index_spaces.find(handle);
        if (finder != created_index_spaces.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_index_spaces.erase(finder);
          else
            return;
        }
        else
          has_created = false;
      }
      if (!has_created)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy index space %x in task %s (UID %lld) "
            "which is not the task that made the index space or one of its "
            "ancestor tasks. Index space deletions must be lexicographically "
            "scoped by the task tree.", handle.get_id(), 
            get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      log_index.debug("Destroying index space %x in task %s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id());
#endif
      std::set<RtEvent> preconditions;
      runtime->forest->destroy_index_space(handle, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    } 

    //--------------------------------------------------------------------------
    void LeafContext::destroy_index_partition(IndexPartition handle,
                                       const bool unordered, const bool recurse)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Check to see if this is one that we should be allowed to destory
      bool has_created = true;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<IndexPartition,unsigned>::iterator finder = 
          created_index_partitions.find(handle);
        if (finder != created_index_partitions.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
          {
            created_index_partitions.erase(finder);
            if (recurse)
            {
              // Remove any other partitions that this partition dominates
              for (std::map<IndexPartition,unsigned>::iterator it = 
                    created_index_partitions.begin(); it !=
                    created_index_partitions.end(); /*nothing*/)
              {
                if ((handle.get_tree_id() == it->first.get_tree_id()) &&
                    runtime->forest->is_dominated_tree_only(it->first, handle))
                {
#ifdef DEBUG_LEGION
                  assert(it->second > 0);
#endif
                  if (--it->second == 0)
                  {
                    std::map<IndexPartition,unsigned>::iterator 
                      to_delete = it++;
                    created_index_partitions.erase(to_delete);
                  }
                  else
                    it++;
                }
                else
                  it++;
              }
            }
          }
          else
            return;
        }
        else
          has_created = false;
      }
      if (!has_created)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy index partition %x in task %s (UID %lld) "
            "which is not the task that made the index space or one of its "
            "ancestor tasks. Index space deletions must be lexicographically "
            "scoped by the task tree.", handle.get_id(), 
            get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      log_index.debug("Destroying index partition %x in task %s (ID %lld)",
                      handle.id, get_task_name(), get_unique_id());
#endif
      std::set<RtEvent> preconditions;
      runtime->forest->destroy_index_partition(handle, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_equal_partition(
                                             IndexSpace parent,
                                             IndexSpace color_space,
                                             size_t granularity, Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_EQUAL_PARTITION_CREATION,
        "Illegal equal partition creation performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_weights(IndexSpace parent,
                                                const FutureMap &weights,
                                                IndexSpace color_space,
                                                size_t granularity, Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_EQUAL_PARTITION_CREATION,
        "Illegal create partition by weights performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_union(
                                          IndexSpace parent,
                                          IndexPartition handle1,
                                          IndexPartition handle2,
                                          IndexSpace color_space,
                                          PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_UNION_PARTITION_CREATION,
        "Illegal union partition creation performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_intersection(
                                                IndexSpace parent,
                                                IndexPartition handle1,
                                                IndexPartition handle2,
                                                IndexSpace color_space,
                                                PartitionKind kind, Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_INTERSECTION_PARTITION_CREATION,
        "Illegal intersection partition creation performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_intersection(
                                                IndexSpace parent,
                                                IndexPartition partition,
                                                PartitionKind kind, Color color,
                                                bool dominates)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_INTERSECTION_PARTITION_CREATION,
        "Illegal intersection partition creation performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_difference(
                                                      IndexSpace parent,
                                                      IndexPartition handle1,
                                                      IndexPartition handle2,
                                                      IndexSpace color_space,
                                                      PartitionKind kind,
                                                      Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_DIFFERENCE_PARTITION_CREATION,
        "Illegal difference partition creation performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    Color LeafContext::create_cross_product_partitions(IndexPartition handle1,
                                                       IndexPartition handle2,
                                   std::map<IndexSpace,IndexPartition> &handles,
                                                       PartitionKind kind,
                                                       Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_CROSS_PRODUCT_PARTITION,
        "Illegal create cross product partitions performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return 0;
    }

    //--------------------------------------------------------------------------
    void LeafContext::create_association(LogicalRegion domain,
                                         LogicalRegion domain_parent,
                                         FieldID domain_fid, IndexSpace range,
                                         MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_ASSOCIATION,
        "Illegal create association performed in leaf task "
                     "%s (ID %lld)", get_task_name(),get_unique_id())
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_restricted_partition(
                                                IndexSpace parent,
                                                IndexSpace color_space,
                                                const void *transform,
                                                size_t transform_size,
                                                const void *extent,
                                                size_t extent_size,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_RESTRICTED_PARTITION,
        "Illegal create restricted partition performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_domain(
                                                IndexSpace parent,
                                    const std::map<DomainPoint,Domain> &domains,
                                                IndexSpace color_space,
                                                bool perform_intersections,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_BY_DOMAIN,
          "Illegal create partition by domain performed in leaf "
          "task %s (UID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_domain(
                                                IndexSpace parent,
                                                const FutureMap &domains,
                                                IndexSpace color_space,
                                                bool perform_intersections,
                                                PartitionKind part_kind,
                                                Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_BY_DOMAIN,
          "Illegal create partition by domain performed in leaf "
          "task %s (UID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_field(
                                                LogicalRegion handle,
                                                LogicalRegion parent_priv,
                                                FieldID fid,
                                                IndexSpace color_space,
                                                Color color,
                                                MapperID id, MappingTagID tag,
                                                PartitionKind part_kind)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_FIELD,
        "Illegal partition by field performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_image(
                                              IndexSpace handle,
                                              LogicalPartition projection,
                                              LogicalRegion parent,
                                              FieldID fid,
                                              IndexSpace color_space,
                                              PartitionKind part_kind,
                                              Color color,
                                              MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_IMAGE,
        "Illegal partition by image performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_image_range(
                                              IndexSpace handle,
                                              LogicalPartition projection,
                                              LogicalRegion parent,
                                              FieldID fid,
                                              IndexSpace color_space,
                                              PartitionKind part_kind,
                                              Color color,
                                              MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_IMAGE_RANGE,
        "Illegal partition by image range performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_preimage(
                                                IndexPartition projection,
                                                LogicalRegion handle,
                                                LogicalRegion parent,
                                                FieldID fid,
                                                IndexSpace color_space,
                                                PartitionKind part_kind,
                                                Color color,
                                                MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_PREIMAGE,
        "Illegal partition by preimage performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_partition_by_preimage_range(
                                                IndexPartition projection,
                                                LogicalRegion handle,
                                                LogicalRegion parent,
                                                FieldID fid,
                                                IndexSpace color_space,
                                                PartitionKind part_kind,
                                                Color color,
                                                MapperID id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_PARTITION_PREIMAGE_RANGE,
        "Illegal partition by preimage range performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexPartition LeafContext::create_pending_partition(
                                              IndexSpace parent,
                                              IndexSpace color_space,
                                              PartitionKind part_kind,
                                              Color color)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_PENDING_PARTITION,
        "Illegal create pending partition performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexPartition::NO_PART;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space_union(IndexPartition parent,
                                                     const void *realm_color,
                                                     size_t color_size,
                                                     TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles) 
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_INDEX_SPACE_UNION,
        "Illegal create index space union performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space_union(IndexPartition parent,
                                                     const void *realm_color,
                                                     size_t color_size,
                                                     TypeTag type_tag,
                                                     IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_INDEX_SPACE_UNION,
        "Illegal create index space union performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space_intersection(
                                                     IndexPartition parent,
                                                     const void *realm_color,
                                                     size_t color_size,
                                                     TypeTag type_tag,
                                        const std::vector<IndexSpace> &handles) 
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_INDEX_SPACE_INTERSECTION,
        "Illegal create index space intersection performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space_intersection(
                                                     IndexPartition parent,
                                                     const void *realm_color,
                                                     size_t color_size,
                                                     TypeTag type_tag,
                                                     IndexPartition handle)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_INDEX_SPACE_INTERSECTION,
        "Illegal create index space intersection performed in "
                     "leaf task %s (ID %lld)", get_task_name(),get_unique_id())
      return IndexSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    IndexSpace LeafContext::create_index_space_difference(
                                                  IndexPartition parent,
                                                  const void *realm_color,
                                                  size_t color_size,
                                                  TypeTag type_tag,
                                                  IndexSpace initial,
                                          const std::vector<IndexSpace> &handles)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_CREATE_INDEX_SPACE_DIFFERENCE,
        "Illegal create index space difference performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return IndexSpace::NO_SPACE;
    } 

    //--------------------------------------------------------------------------
    FieldSpace LeafContext::create_field_space(const std::vector<Future> &sizes,
                                         std::vector<FieldID> &resulting_fields,
                                         CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_NONLOCAL_FIELD_ALLOCATION2,
       "Illegal deferred field allocations performed in leaf task %s (ID %lld)",
       get_task_name(), get_unique_id())
      return FieldSpace::NO_SPACE;
    }

    //--------------------------------------------------------------------------
    void LeafContext::destroy_field_space(FieldSpace handle, 
                                          const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      // Check to see if this is one that we should be allowed to destory
      bool has_created = true;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<FieldSpace,unsigned>::iterator finder = 
          created_field_spaces.find(handle);
        if (finder != created_field_spaces.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_field_spaces.erase(finder);
          else
            return;
        }
        else
          has_created = false;
      }
      if (!has_created)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy field space %x in task %s (UID %lld) "
            "which is not the task that made the field space or one of its "
            "ancestor tasks. Field space deletions must be lexicographically "
            "scoped by the task tree.", handle.get_id(), 
            get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      log_field.debug("Destroying field space %x in task %s (ID %lld)", 
                      handle.id, get_task_name(), get_unique_id());
#endif
      std::set<RtEvent> preconditions;
      runtime->forest->destroy_field_space(handle, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    }

    //--------------------------------------------------------------------------
    void LeafContext::free_field(FieldAllocatorImpl *allocator,FieldSpace space, 
                                 FieldID fid, const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      bool has_created = true;
      {
        AutoLock priv_lock(privilege_lock);
        const std::pair<FieldSpace,FieldID> key(space, fid);
        std::set<std::pair<FieldSpace,FieldID> >::iterator finder = 
          created_fields.find(key);
        if (finder != created_fields.end())
          created_fields.erase(finder);
        else // No need to check for local fields since we can't make them
          has_created = false;
      }
      if (!has_created)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to deallocate field %d in field space %x in task %s "
            "(UID %lld) which is not the task that allocated the field "
            "or one of its ancestor tasks. Field deallocations must be " 
            "lexicographically scoped by the task tree.", fid, space.id,
            get_task_name(), get_unique_id())
      // If the allocator is not ready we need to wait for it here
      if (allocator->ready_event.exists() && 
          !allocator->ready_event.has_triggered())
        allocator->ready_event.wait();
      // Free the indexes first and immediately
      std::vector<FieldID> to_free(1,fid);
      runtime->forest->free_field_indexes(space, to_free, RtEvent::NO_RT_EVENT);
      // We can free this field immediately
      std::set<RtEvent> preconditions;
      runtime->forest->free_field(space, fid, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    }

    //--------------------------------------------------------------------------
    void LeafContext::free_fields(FieldAllocatorImpl *allocator, 
                                  FieldSpace space, 
                                  const std::set<FieldID> &to_free,
                                  const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      long bad_fid = -1;
      {
        AutoLock priv_lock(privilege_lock);
        for (std::set<FieldID>::const_iterator it = 
              to_free.begin(); it != to_free.end(); it++)
        {
          const std::pair<FieldSpace,FieldID> key(space, *it);
          std::set<std::pair<FieldSpace,FieldID> >::iterator finder = 
            created_fields.find(key);
          if (finder == created_fields.end())
          {
            // No need to check for local fields since we know
            // that leaf tasks are not allowed to make them
            bad_fid = *it;
            break;
          }
          else
            created_fields.erase(finder);
        }
      }
      if (bad_fid != -1)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to deallocate field %ld in field space %x in task %s "
            "(UID %lld) which is not the task that allocated the field "
            "or one of its ancestor tasks. Field deallocations must be " 
            "lexicographically scoped by the task tree.", bad_fid, space.id,
            get_task_name(), get_unique_id())
      // If the allocator is not ready we need to wait for it here
      if (allocator->ready_event.exists() && 
          !allocator->ready_event.has_triggered())
        allocator->ready_event.wait();
      // Free the indexes first and immediately
      const std::vector<FieldID> field_vec(to_free.begin(), to_free.end());
      runtime->forest->free_field_indexes(space,field_vec,RtEvent::NO_RT_EVENT);
      // We can free these fields immediately
      std::set<RtEvent> preconditions;
      runtime->forest->free_fields(space, field_vec, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    }

    //--------------------------------------------------------------------------
    FieldID LeafContext::allocate_field(FieldSpace space, 
                                        const Future &field_size,
                                        FieldID fid, bool local,
                                        CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_NONLOCAL_FIELD_ALLOCATION,
        "Illegal deferred field allocation performed in leaf task %s (ID %lld)",
        get_task_name(), get_unique_id())
      return 0;
    }

    //--------------------------------------------------------------------------
    void LeafContext::allocate_local_field(FieldSpace space, size_t field_size,
                                     FieldID fid, CustomSerdezID serdez_id,
                                     std::set<RtEvent> &done_events)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_NONLOCAL_FIELD_ALLOCATION,
          "Illegal local field allocation performed in leaf task %s (ID %lld)",
          get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::allocate_fields(FieldSpace space,
                                      const std::vector<Future> &sizes,
                                      std::vector<FieldID> &resuling_fields,
                                      bool local, CustomSerdezID serdez_id)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_NONLOCAL_FIELD_ALLOCATION2,
       "Illegal deferred field allocations performed in leaf task %s (ID %lld)",
       get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::allocate_local_fields(FieldSpace space,
                                   const std::vector<size_t> &sizes,
                                   const std::vector<FieldID> &resuling_fields,
                                   CustomSerdezID serdez_id,
                                   std::set<RtEvent> &done_events)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_NONLOCAL_FIELD_ALLOCATION2,
          "Illegal local field allocations performed in leaf task %s (ID %lld)",
          get_task_name(), get_unique_id())
    } 

    //--------------------------------------------------------------------------
    LogicalRegion LeafContext::create_logical_region(RegionTreeForest *forest,
                                                      IndexSpace index_space,
                                                      FieldSpace field_space,
                                                      const bool task_local,
                                                      const bool output_region)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      RegionTreeID tid = runtime->get_unique_region_tree_id();
      LogicalRegion region(tid, index_space, field_space);
#ifdef DEBUG_LEGION
      log_region.debug("Creating logical region in task %s (ID %lld) with "
                       "index space %x and field space %x in new tree %d",
                       get_task_name(), get_unique_id(), 
                       index_space.id, field_space.id, tid);
#endif
      if (runtime->legion_spy_enabled)
        LegionSpy::log_top_region(index_space.id, field_space.id, tid);

      const DistributedID did = runtime->get_available_distributed_id();
      forest->create_logical_region(region, did);
      // Register the creation of a top-level region with the context
      register_region_creation(region, task_local, output_region);
      // Don't bother making any equivalence sets yet, we'll do that
      // in the end_task call when we know about only the regions
      // that have survived the execution of the task
      return region;
    }

    //--------------------------------------------------------------------------
    void LeafContext::destroy_logical_region(LogicalRegion handle,
                                             const bool unordered)
    //--------------------------------------------------------------------------
    {
      AutoRuntimeCall call(this);
      if (!handle.exists())
        return;
      // Check to see if this is a top-level logical region, if not then
      // we shouldn't even be destroying it
      if (!runtime->forest->is_top_level_region(handle))
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy logical region (%x,%x,%x in task %s "
            "(UID %lld) which is not a top-level logical region. Legion only "
            "permits top-level logical regions to be destroyed.", 
            handle.index_space.id, handle.field_space.id, handle.tree_id,
            get_task_name(), get_unique_id())
      // Check to see if this is one that we should be allowed to destory
      bool has_created = true;
      {
        AutoLock priv_lock(privilege_lock);
        std::map<LogicalRegion,unsigned>::iterator finder = 
          created_regions.find(handle);
        if (finder != created_regions.end())
        {
#ifdef DEBUG_LEGION
          assert(finder->second > 0);
#endif
          if (--finder->second == 0)
            created_regions.erase(finder);
          else
            return;
        }
        else
          has_created = false;
      }
      if (!has_created)
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_RESOURCE_DESTRUCTION,
            "Illegal call to destroy logical region (%x,%x,%x) in task %s "
            "(UID %lld) which is not the task that made the logical region "
            "or one of its ancestor tasks. Logical region deletions must be " 
            "lexicographically scoped by the task tree.", handle.index_space.id,
            handle.field_space.id, handle.tree_id,
            get_task_name(), get_unique_id())
#ifdef DEBUG_LEGION
      log_region.debug("Deleting logical region (%x,%x) in task %s (ID %lld)",
                       handle.index_space.id, handle.field_space.id, 
                       get_task_name(), get_unique_id());
#endif
      std::set<RtEvent> preconditions;
      runtime->forest->destroy_logical_region(handle, preconditions);
      if (!preconditions.empty())
      {
        AutoLock l_lock(leaf_lock);
        execution_events.insert(preconditions.begin(), preconditions.end());
      }
    }

    //--------------------------------------------------------------------------
    void LeafContext::advise_analysis_subtree(LogicalRegion parent,
                                   const std::set<LogicalRegion> &regions,
                                   const std::set<LogicalPartition> &partitions,
                                   const std::set<FieldID> &fields)
    //--------------------------------------------------------------------------
    {
      // No-op
    }

    //--------------------------------------------------------------------------
    void LeafContext::get_local_field_set(const FieldSpace handle,
                                          const std::set<unsigned> &indexes,
                                          std::set<FieldID> &to_set) const
    //--------------------------------------------------------------------------
    {
      // Should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::get_local_field_set(const FieldSpace handle,
                                          const std::set<unsigned> &indexes,
                                          std::vector<FieldID> &to_set) const
    //--------------------------------------------------------------------------
    {
      // Should never be called
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::add_physical_region(const RegionRequirement &req,
          bool mapped, MapperID mid, MappingTagID tag, ApUserEvent &unmap_event,
          bool virtual_mapped, const InstanceSet &physical_instances)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(!unmap_event.exists());
#endif
      PhysicalRegionImpl *impl = new PhysicalRegionImpl(req, 
          RtEvent::NO_RT_EVENT, ApEvent::NO_AP_EVENT, 
          ApUserEvent::NO_AP_USER_EVENT, mapped, this, mid, tag, 
          true/*leaf region*/, virtual_mapped, runtime);
      physical_regions.push_back(PhysicalRegion(impl));
      if (mapped)
        impl->set_references(physical_instances, true/*safe*/);
    }

    //--------------------------------------------------------------------------
    Future LeafContext::execute_task(const TaskLauncher &launcher,
                                     std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (launcher.enable_inlining)
      {
        if (launcher.predicate == Predicate::FALSE_PRED)
          return predicate_task_false(launcher);
        IndividualTask *task = runtime->get_available_individual_task(); 
        InnerContext *parent = owner_task->get_context();
        Future result = task->initialize_task(parent, launcher, outputs);
        inline_child_task(task);
        return result;
      }
      else
      {
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_EXECUTE_TASK_CALL,
          "Illegal execute task call performed in leaf task %s "
                       "(ID %lld)", get_task_name(), get_unique_id())
        return Future();
      }
    }

    //--------------------------------------------------------------------------
    FutureMap LeafContext::execute_index_space(
                                        const IndexTaskLauncher &launcher,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (!launcher.must_parallelism && launcher.enable_inlining)
      {
        if (launcher.predicate == Predicate::FALSE_PRED)
          return predicate_index_task_false(++inlined_tasks, launcher); 
        IndexTask *task = runtime->get_available_index_task();
        InnerContext *parent = owner_task->get_context();
        IndexSpace launch_space = launcher.launch_space;
        if (!launch_space.exists())
          launch_space = find_index_launch_space(launcher.launch_domain);
        FutureMap result = 
          task->initialize_task(parent, launcher, launch_space, outputs);
        inline_child_task(task);
        return result;
      }
      else
      {
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_EXECUTE_INDEX_SPACE,
          "Illegal execute index space call performed in leaf "
                       "task %s (ID %lld)", get_task_name(), get_unique_id())
        return FutureMap();
      }
    }

    //--------------------------------------------------------------------------
    Future LeafContext::execute_index_space(const IndexTaskLauncher &launcher,
                                        ReductionOpID redop, bool deterministic,
                                        std::vector<OutputRequirement> *outputs)
    //--------------------------------------------------------------------------
    {
      if (!launcher.must_parallelism && launcher.enable_inlining)
      {
        if (launcher.predicate == Predicate::FALSE_PRED)
          return predicate_index_task_reduce_false(launcher);
        IndexTask *task = runtime->get_available_index_task();
        InnerContext *parent = owner_task->get_context();
        IndexSpace launch_space = launcher.launch_space;
        if (!launch_space.exists())
          launch_space = find_index_launch_space(launcher.launch_domain);
        Future result = task->initialize_task(parent, launcher, launch_space, 
                                              redop, deterministic, outputs);
        inline_child_task(task);
        return result;
      }
      else
      {
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_EXECUTE_INDEX_SPACE,
          "Illegal execute index space call performed in leaf "
                       "task %s (ID %lld)", get_task_name(), get_unique_id())
        return Future();
      }
    }

    //--------------------------------------------------------------------------
    Future LeafContext::reduce_future_map(const FutureMap &future_map,
                                        ReductionOpID redop, bool deterministic,
                                        MapperID mapper_id, MappingTagID tag)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_EXECUTE_INDEX_SPACE,
        "Illegal reduce future map call performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    FutureMap LeafContext::construct_future_map(const Domain &domain,
                                    const std::map<DomainPoint,Future> &futures,
                                    RtUserEvent domain_deletion, bool internal)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_EXECUTE_INDEX_SPACE,
        "Illegal construct future map call performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return FutureMap();
    }

    //--------------------------------------------------------------------------
    PhysicalRegion LeafContext::map_region(const InlineLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_MAP_REGION,
        "Illegal map_region operation performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
      return PhysicalRegion();
    }

    //--------------------------------------------------------------------------
    ApEvent LeafContext::remap_region(PhysicalRegion region)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_REMAP_OPERATION,
        "Illegal remap operation performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
      return ApEvent::NO_AP_EVENT;
    }

    //--------------------------------------------------------------------------
    void LeafContext::unmap_region(PhysicalRegion region)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_UNMAP_OPERATION,
        "Illegal unmap operation performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::unmap_all_regions(bool external)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_UNMAP_OPERATION,
        "Illegal unmap_all_regions call performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::fill_fields(const FillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_FILL_OPERATION_CALL,
        "Illegal fill operation call performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::fill_fields(const IndexFillLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_INDEX_FILL_OPERATION_CALL,
        "Illegal index fill operation call performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::issue_copy(const CopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_COPY_FILL_OPERATION_CALL,
        "Illegal copy operation call performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::issue_copy(const IndexCopyLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_INDEX_COPY_OPERATION,
        "Illegal index copy operation call performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::issue_acquire(const AcquireLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_ACQUIRE_OPERATION,
        "Illegal acquire operation performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::issue_release(const ReleaseLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_RELEASE_OPERATION,
        "Illegal release operation performed in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    PhysicalRegion LeafContext::attach_resource(const AttachLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_ATTACH_RESOURCE_OPERATION,
        "Illegal attach resource operation performed in leaf "
                     "task %s (ID %lld)", get_task_name(), get_unique_id())
      return PhysicalRegion();
    }
    
    //--------------------------------------------------------------------------
    Future LeafContext::detach_resource(PhysicalRegion region, const bool flush,
                                        const bool unordered)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_DETACH_RESOURCE_OPERATION,
        "Illegal detach resource operation performed in leaf "
                      "task %s (ID %lld)", get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    void LeafContext::progress_unordered_operations(void)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_DETACH_RESOURCE_OPERATION,
        "Illegal progress unordered operations performed in leaf "
                      "task %s (ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    FutureMap LeafContext::execute_must_epoch(const MustEpochLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_EXECUTE_MUST_EPOCH,
        "Illegal Legion execute must epoch call in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
      return FutureMap();
    }

    //--------------------------------------------------------------------------
    Future LeafContext::issue_timing_measurement(const TimingLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_TIMING_MEASUREMENT,
        "Illegal timing measurement operation in leaf task %s"
                     "(ID %lld)", get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    Future LeafContext::issue_mapping_fence(void)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_MAPPING_FENCE_CALL,
        "Illegal legion mapping fence call in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    Future LeafContext::issue_execution_fence(void)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_EXECUTION_FENCE_CALL,
        "Illegal Legion execution fence call in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
      return Future();
    }

    //--------------------------------------------------------------------------
    void LeafContext::complete_frame(void)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_COMPLETE_FRAME_CALL,
        "Illegal Legion complete frame call in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    Predicate LeafContext::create_predicate(const Future &f)
    //--------------------------------------------------------------------------
    {
      if (f.impl == NULL)
        return Predicate::FALSE_PRED;
      const RtEvent ready = 
        f.impl->request_internal_buffer(owner_task, true/*eager*/);
      if (ready.exists() && !ready.has_triggered())
        ready.wait();
      // Always eagerly evaluate predicates in leaf contexts
      const bool value = f.impl->get_boolean_value(this);
      if (value)
        return Predicate::TRUE_PRED;
      else
        return Predicate::FALSE_PRED;
    }

    //--------------------------------------------------------------------------
    Predicate LeafContext::predicate_not(const Predicate &p)
    //--------------------------------------------------------------------------
    {
      if (p == Predicate::TRUE_PRED)
        return Predicate::FALSE_PRED;
      else if (p == Predicate::FALSE_PRED)
        return Predicate::TRUE_PRED;
      else // should never get here, all predicates should be eagerly evaluated
        assert(false);  
      return Predicate::TRUE_PRED;
    }
    
    //--------------------------------------------------------------------------
    Predicate LeafContext::create_predicate(const PredicateLauncher &launcher)
    //--------------------------------------------------------------------------
    {
      if (launcher.predicates.empty())
        REPORT_LEGION_ERROR(ERROR_ILLEGAL_PREDICATE_CREATION,
          "Illegal predicate creation performed on a "
                      "set of empty previous predicates in task %s (ID %lld).",
                      get_task_name(), get_unique_id())
      else if (launcher.predicates.size() == 1)
        return launcher.predicates[0];
      if (launcher.and_op)
      {
        // Check for short circuit cases
        for (std::vector<Predicate>::const_iterator it = 
              launcher.predicates.begin(); it != 
              launcher.predicates.end(); it++)
        {
          if ((*it) == Predicate::FALSE_PRED)
            return Predicate::FALSE_PRED;
          else if ((*it) == Predicate::TRUE_PRED)
            continue;
          else // should never get here, 
            // all predicates should be eagerly evaluated
            assert(false);
        }
        return Predicate::TRUE_PRED;
      }
      else
      {
        // Check for short circuit cases
        for (std::vector<Predicate>::const_iterator it = 
              launcher.predicates.begin(); it != 
              launcher.predicates.end(); it++)
        {
          if ((*it) == Predicate::TRUE_PRED)
            return Predicate::TRUE_PRED;
          else if ((*it) == Predicate::FALSE_PRED)
            continue;
          else // should never get here, 
            // all predicates should be eagerly evaluated
            assert(false);
        }
        return Predicate::FALSE_PRED;
      }
    }

    //--------------------------------------------------------------------------
    Future LeafContext::get_predicate_future(const Predicate &p)
    //--------------------------------------------------------------------------
    {
      if (p == Predicate::TRUE_PRED)
      {
        Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
        const bool value = true;
        result.impl->set_local(&value, sizeof(value));
        return result;
      }
      else if (p == Predicate::FALSE_PRED)
      {
        Future result = runtime->help_create_future(ApEvent::NO_AP_EVENT);
        const bool value = false;
        result.impl->set_local(&value, sizeof(value));
        return result;
      }
      else // should never get here, all predicates should be eagerly evaluated
        assert(false);
      return Future();
    }

    //--------------------------------------------------------------------------
    ApBarrier LeafContext::create_phase_barrier(unsigned arrivals,
                                                ReductionOpID redop,
                                                const void *init_value,
                                                size_t init_size)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      log_run.debug("Creating application barrier in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      return ApBarrier(Realm::Barrier::create_barrier(arrivals, redop,
                                                      init_value, init_size));
    }

    //--------------------------------------------------------------------------
    void LeafContext::destroy_phase_barrier(ApBarrier bar)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      log_run.debug("Destroying phase barrier in task %s (ID %lld)",
                      get_task_name(), get_unique_id());
#endif
      destroy_user_barrier(bar);
    }

    //--------------------------------------------------------------------------
    PhaseBarrier LeafContext::advance_phase_barrier(PhaseBarrier bar)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
          "Illegal advance phase barrier call performed in leaf task %s "
          "(UID %lld)", get_task_name(), get_unique_id());
      return bar;
    }

    //--------------------------------------------------------------------------
    void LeafContext::arrive_dynamic_collective(DynamicCollective dc,
                                                const void *buffer,
                                                size_t size, unsigned count)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
          "Illegal arrive dynamic collective call performed in leaf task %s "
          "(UID %lld)", get_task_name(), get_unique_id());
    }

    //--------------------------------------------------------------------------
    void LeafContext::defer_dynamic_collective_arrival(DynamicCollective dc,
                                                       const Future &f,
                                                       unsigned count)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
          "Illegal defer dynamic collective call performed in leaf task %s "
          "(UID %lld)", get_task_name(), get_unique_id());
    }

    //--------------------------------------------------------------------------
    Future LeafContext::get_dynamic_collective_result(DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
          "Illegal get dynamic collective result call performed in leaf task %s"
          " (UID %lld)", get_task_name(), get_unique_id());
      return Future();
    }

    //--------------------------------------------------------------------------
    DynamicCollective LeafContext::advance_dynamic_collective( 
                                                           DynamicCollective dc)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_LEAF_TASK_VIOLATION,
          "Illegal advance dynamic collective call performed in leaf task %s "
          "(UID %lld)", get_task_name(), get_unique_id());
      return dc;
    }

    //--------------------------------------------------------------------------
    size_t LeafContext::register_new_child_operation(Operation *op,
                    const std::vector<StaticDependence> *dependences)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(op->get_operation_kind() == Operation::TASK_OP_KIND);
#endif
      return ++inlined_tasks;
    }

    //--------------------------------------------------------------------------
    void LeafContext::register_new_internal_operation(InternalOp *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    size_t LeafContext::register_new_close_operation(CloseOp *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return 0;
    }

    //--------------------------------------------------------------------------
    size_t LeafContext::register_new_summary_operation(TraceSummaryOp *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return 0;
    }

    //--------------------------------------------------------------------------
    ApEvent LeafContext::add_to_dependence_queue(Operation *op, 
                                                 bool unordered, bool block)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return ApEvent::NO_AP_EVENT;
    }

    //--------------------------------------------------------------------------
    void LeafContext::register_executing_child(Operation *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::register_child_executed(Operation *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::register_child_complete(Operation *op)
    //--------------------------------------------------------------------------
    {
      // Nothing to do
    }

    //--------------------------------------------------------------------------
    void LeafContext::register_child_commit(Operation *op)
    //--------------------------------------------------------------------------
    {
      // Nothing to do
    }

    //--------------------------------------------------------------------------
    ApEvent LeafContext::register_implicit_dependences(Operation *op)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return ApEvent::NO_AP_EVENT;
    }

    //--------------------------------------------------------------------------
    void LeafContext::perform_fence_analysis(Operation *op,
                 std::set<ApEvent> &preconditions, bool mapping, bool execution)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::update_current_fence(FenceOp *op, 
                                           bool mapping, bool execution)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::update_current_implicit(Operation *op) 
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    RtEvent LeafContext::get_current_mapping_fence_event(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    ApEvent LeafContext::get_current_execution_fence_event(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return ApEvent::NO_AP_EVENT;
    }

    //--------------------------------------------------------------------------
    void LeafContext::begin_trace(TraceID tid, bool logical_only,
        bool static_trace, const std::set<RegionTreeID> *trees, bool deprecated)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_BEGIN_TRACE,
        "Illegal Legion begin trace call in leaf task %s "
                     "(ID %lld)", get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::end_trace(TraceID tid, bool deprecated)
    //--------------------------------------------------------------------------
    {
      REPORT_LEGION_ERROR(ERROR_ILLEGAL_LEGION_END_TRACE,
        "Illegal Legion end trace call in leaf task %s (ID %lld)",
                     get_task_name(), get_unique_id())
    }

    //--------------------------------------------------------------------------
    void LeafContext::record_previous_trace(LegionTrace *trace)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(false);
#endif
      exit(ERROR_LEAF_TASK_VIOLATION);
    }

    //--------------------------------------------------------------------------
    void LeafContext::invalidate_trace_cache(
                                     LegionTrace *trace, Operation *invalidator)
    //--------------------------------------------------------------------------
    {
#ifdef DEBUG_LEGION
      assert(false);
#endif
      exit(ERROR_LEAF_TASK_VIOLATION);
    }

    //--------------------------------------------------------------------------
    void LeafContext::record_blocking_call(void)
    //--------------------------------------------------------------------------
    {
    }

    //--------------------------------------------------------------------------
    void LeafContext::issue_frame(FrameOp *frame, ApEvent frame_termination)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::perform_frame_issue(FrameOp *frame, ApEvent frame_term)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::finish_frame(ApEvent frame_termination)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::increment_outstanding(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::decrement_outstanding(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::increment_pending(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    RtEvent LeafContext::decrement_pending(TaskOp *child)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    RtEvent LeafContext::decrement_pending(bool need_deferral)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return RtEvent::NO_RT_EVENT;
    }

    //--------------------------------------------------------------------------
    void LeafContext::increment_frame(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::decrement_frame(void)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    MergeCloseOp* LeafContext::get_merge_close_op(const LogicalUser &user,
                                                  RegionTreeNode *node)
#else
    MergeCloseOp* LeafContext::get_merge_close_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
#ifdef DEBUG_LEGION_COLLECTIVES
    RefinementOp* LeafContext::get_refinement_op(const LogicalUser &user,
                                                 RegionTreeNode *node)
#else
    RefinementOp* LeafContext::get_refinement_op(void)
#endif
    //--------------------------------------------------------------------------
    {
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
    InnerContext* LeafContext::find_parent_physical_context(unsigned index)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
    InnerContext* LeafContext::find_top_context(InnerContext *previous)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
    void LeafContext::initialize_region_tree_contexts(
                       const std::vector<RegionRequirement> &clone_requirements,
                       const LegionVector<VersionInfo>::aligned &version_infos,
                       const std::vector<EquivalenceSet*> &equivalence_sets,
                       const std::vector<ApUserEvent> &unmap_events,
                       std::set<RtEvent> &applied_events,
                       std::set<RtEvent> &execution_events)
    //--------------------------------------------------------------------------
    {
      // Nothing to do
    }

    //--------------------------------------------------------------------------
    void LeafContext::invalidate_region_tree_contexts(
                       const bool is_top_level_task, std::set<RtEvent> &applied)
    //--------------------------------------------------------------------------
    {
      // Nothing to do 
    }

    //--------------------------------------------------------------------------
    void LeafContext::receive_created_region_contexts(RegionTreeContext ctx,
                           const std::vector<RegionNode*> &created_states,
                           std::set<RtEvent> &applied_events, size_t num_shards,
                           InnerContext *source_context)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::free_region_tree_context(void)
    //--------------------------------------------------------------------------
    {
      // Nothing to do 
    }

    //--------------------------------------------------------------------------
    InstanceView* LeafContext::create_instance_top_view(
                                PhysicalManager *manager, AddressSpaceID source)
    //--------------------------------------------------------------------------
    {
      assert(false);
      return NULL;
    }

    //--------------------------------------------------------------------------
    void LeafContext::end_task(const void *res, size_t res_size, bool owned,
     PhysicalInstance deferred_result_instance, FutureFunctor *callback_functor,
                       Memory::Kind result_kind, void (*freefunc)(void*,size_t))
    //--------------------------------------------------------------------------
    {
      // No local regions or fields permitted in leaf tasks
      if (overhead_tracker != NULL)
      {
        const long long current = Realm::Clock::current_time_in_nanoseconds();
        const long long diff = current - previous_profiling_time;
        overhead_tracker->application_time += diff;
      }
      // No need to unmap the physical regions, they never had events
      if (!execution_events.empty())
      {
        const RtEvent wait_on = Runtime::merge_events(execution_events);
        wait_on.wait();
      } 
      TaskContext::end_task(res, res_size, owned, deferred_result_instance,
                            callback_functor, result_kind, freefunc); 
    }

    //--------------------------------------------------------------------------
    void LeafContext::post_end_task(FutureInstance *instance,
                                    FutureFunctor *callback_functor,
                                    bool own_callback_functor)
    //--------------------------------------------------------------------------
    {
      // Safe to cast to a single task here because this will never
      // be called while inlining an index space task
      // Handle the future result
      owner_task->handle_future(instance, callback_functor,
                                executing_processor, own_callback_functor);
      bool need_complete = false;
      bool need_commit = false;
      {
        AutoLock leaf(leaf_lock);
#ifdef DEBUG_LEGION
        assert(!task_executed);
#endif
        // Now that we know the last registration has taken place we
        // can mark that we are done executing
        task_executed = true;
        if (!children_complete_invoked)
        {
          need_complete = true;
          children_complete_invoked = true;
        }
        if (!children_commit_invoked)
        {
          need_commit = true;
          children_commit_invoked = true;
        }
      } 
      if (need_complete)
        owner_task->trigger_children_complete(ApEvent::NO_AP_EVENT);
      if (need_commit)
        owner_task->trigger_children_committed();
    }

    //--------------------------------------------------------------------------
    void LeafContext::record_dynamic_collective_contribution(
                                          DynamicCollective dc, const Future &f) 
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    void LeafContext::find_collective_contributions(DynamicCollective dc, 
                                             std::vector<Future> &contributions) 
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

    //--------------------------------------------------------------------------
    TaskPriority LeafContext::get_current_priority(void) const
    //--------------------------------------------------------------------------
    {
      assert(false);
      return 0;
    }

    //--------------------------------------------------------------------------
    void LeafContext::set_current_priority(TaskPriority priority)
    //--------------------------------------------------------------------------
    {
      assert(false);
    }

  };
};

// EOF

