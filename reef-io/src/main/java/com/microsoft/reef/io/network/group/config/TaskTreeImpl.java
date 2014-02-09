/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.config;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.IdentifierFactory;

/**
 *
 */
public class TaskTreeImpl implements TaskTree {

  private static class IdentifierStatus {

    public final ComparableIdentifier id;
    public Status status;

    @Override
    public String toString() {
      return "(" + id + ", " + status + ")";
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == null || !(obj instanceof IdentifierStatus))
        return false;
      if (obj == this)
        return true;
      final IdentifierStatus that = (IdentifierStatus) obj;
      if (this.status != Status.ANY && that.status != Status.ANY) {
        return this.id.equals(that.id) && this.status.equals(that.status);
      }
      else {
        return this.id.equals(that.id);
      }
    }

    public IdentifierStatus(final ComparableIdentifier id) {
      super();
      this.id = id;
      this.status = Status.UNSCHEDULED;
    }

    public IdentifierStatus(final ComparableIdentifier id, final Status status) {
      super();
      this.id = id;
      this.status = status;
    }

    public static IdentifierStatus any(final ComparableIdentifier id) {
      return new IdentifierStatus(id, Status.ANY);
    }
  }

  private final List<IdentifierStatus> tasks = new ArrayList<>();
  private final TreeSet<Integer> holes = new TreeSet<>();

  @Override
  public synchronized void add(final ComparableIdentifier id) {
    System.out.println("Before Tasks: " + tasks);
//    System.out.println("Before Holes: " + holes);
    if(holes.isEmpty()){
//      System.out.println("No holes. Adding at the end");
      tasks.add(new IdentifierStatus(id));
    }
    else{
//      System.out.println("Found holes: " + holes);
      int firstHole = holes.first();
//      System.out.println("First hole at " + firstHole + ". Removing it");
      holes.remove(firstHole);
//      System.out.println("Adding " + id + " at " + firstHole);
      tasks.add(firstHole, new IdentifierStatus(id));
    }
    System.out.println("After Tasks: " + tasks);
//    System.out.println("After Holes: " + holes);
  }

  @Override
  public String toString() {
    return tasks.toString();
  }

  @Override
  public ComparableIdentifier parent(final ComparableIdentifier id) {
    final int idx = tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1 || idx == 0)
      return null;
    int parIdx = (idx - 1) / 2;
    try {
      return tasks.get(parIdx).id;
    }catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public ComparableIdentifier left(final ComparableIdentifier id) {
    final int idx = tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1)
      return null;
    final int leftIdx = idx * 2 + 1;
    try {
      return tasks.get(leftIdx).id;
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public ComparableIdentifier right(final ComparableIdentifier id) {
    final int idx = tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1)
      return null;
    final int rightIdx = idx * 2 + 2;
    try {
      return tasks.get(rightIdx).id;
    }catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public List<ComparableIdentifier> neighbors(final ComparableIdentifier id) {
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    final ComparableIdentifier parent = parent(id);
    if (parent != null)
      retVal.add(parent);
    retVal.addAll(children(id));
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> children(final ComparableIdentifier id) {
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    final ComparableIdentifier left = left(id);
    if (left != null) {
      retVal.add(left);
      final ComparableIdentifier right = right(id);
      if (right!=null)
        retVal.add(right);
    }
    return retVal;
  }

  @Override
  public int childrenSupported(final ComparableIdentifier taskIdId) {
    return 2;
  }

  @Override
  public synchronized void remove(final ComparableIdentifier failedTaskId) {
    final int hole = tasks.indexOf(IdentifierStatus.any(failedTaskId));
    if (hole != -1) {
      holes.add(hole);
      tasks.remove(hole);
    }
  }

  @Override
  public List<ComparableIdentifier> scheduledChildren(final ComparableIdentifier taskId) {
    final List<ComparableIdentifier> children = children(taskId);
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    for (final ComparableIdentifier child : children) {
      Status s = getStatus(child);
      if (Status.SCHEDULED == s)
        retVal.add(child);
    }
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> scheduledNeighbors(final ComparableIdentifier taskId) {
    final List<ComparableIdentifier> neighbors = neighbors(taskId);
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    for (final ComparableIdentifier neighbor : neighbors) {
      Status s = getStatus(neighbor);
      if (Status.SCHEDULED == s)
        retVal.add(neighbor);
    }
    return retVal;
  }

  @Override
  public void setStatus(final ComparableIdentifier taskId, final Status status) {
    final int idx = tasks.indexOf(IdentifierStatus.any(taskId));
    if (idx != -1) {
      tasks.get(idx).status = status;
    }
  }

  @Override
  public Status getStatus(final ComparableIdentifier taskId) {
    final int idx = tasks.indexOf(IdentifierStatus.any(taskId));
    return idx == -1 ? null : tasks.get(idx).status;
  }

  public static void main(final String[] args) {
    final IdentifierFactory idFac = new StringIdentifierFactory();
    final TaskTree tree = new TaskTreeImpl();
    final ComparableIdentifier[] ids = new ComparableIdentifier[7];
    for(int i = 0; i < ids.length; ++i) {
      ids[i] = (ComparableIdentifier) idFac.getNewInstance(Integer.toString(i));
    }
    tree.add(ids[0]);
    tree.add(ids[2]);
    tree.add(ids[1]);
    tree.add(ids[6]);
    tree.add(ids[4]);
    tree.add(ids[5]);
    //tree.add(ids[3]);

    tree.setStatus(ids[2], Status.SCHEDULED);

    System.out.println(tree);
    System.out.println("Children");
    System.out.println(tree.children(ids[0]));
    System.out.println("Sched Children");
    System.out.println(tree.scheduledChildren(ids[0]));
    System.out.println();

    Queue<ComparableIdentifier> idss = new LinkedList<>();
    idss.add((ComparableIdentifier) idFac.getNewInstance("0"));
    while(!idss.isEmpty()){
      ComparableIdentifier id = idss.poll();
      System.out.println(id);
      ComparableIdentifier left = tree.left(id);
      if(left!=null){
        idss.add(left);
        ComparableIdentifier right = tree.right(id);
        if(right!=null)
          idss.add(right);
      }
    }
    tree.setStatus(ids[4], Status.SCHEDULED);
    System.out.println(tree);
    System.out.println("Neighbors");
    System.out.println(tree.neighbors(ids[2]));
    System.out.println("Sched Neighbors");
    System.out.println(tree.scheduledNeighbors(ids[2]));
  }
}
